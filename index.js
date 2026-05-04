import express from "express";
import mysql from "mysql2/promise";
import Redis from "ioredis";

const app = express();
app.use((req, res, next) => {
  // res.header("Access-Control-Allow-Origin", "*");
  // res.header("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS");
  // res.header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With");
  if (req.method === "OPTIONS") {
    return res.sendStatus(204);
  }
  next();
});
app.use(express.json());
app.use((err, req, res, next) => {
  if (err && err.type === "entity.parse.failed") {
    console.error("[process-tryout-user] invalid JSON payload", {
      at: new Date().toISOString(),
      path: req.originalUrl,
      method: req.method,
      ip: req.ip,
      error: err.message,
    });
    return res.status(400).json({
      success: false,
      message: "Payload JSON tidak valid",
    });
  }
  next(err);
});
const redis = new Redis();

// koneksi pool database
const pool = mysql.createPool({
  host: "localhost",
  user:'fikar',
  password:'fikar123',
  database:'siapptn',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

const AUTO_WEIGHT_MIN_TOTAL = 9.5;
const AUTO_WEIGHT_MAX_TOTAL = 10;
const AUTO_WEIGHT_TARGET_TOTAL = 9.5;

function normalizeJenis(jenis = "") {
  return (jenis || "").toString().trim().toLowerCase();
}

function isKedinasanJenis(jenis = "") {
  const normalized = normalizeJenis(jenis);
  return normalized === "kedinasan" || normalized === "cpns" || normalized === "skd";
}

function isUmUgmJenis(jenis = "") {
  return normalizeJenis(jenis) === "um ugm";
}

function logProcessTryoutUser(level = "info", message = "", context = {}) {
  const payload = {
    at: new Date().toISOString(),
    ...context,
  };
  if (level === "error") {
    console.error(`[process-tryout-user] ${message}`, payload);
    return;
  }
  if (level === "warn") {
    console.warn(`[process-tryout-user] ${message}`, payload);
    return;
  }
  console.log(`[process-tryout-user] ${message}`, payload);
}

function userNotAttemptedResponse(message = "Kamu belum mengerjakan tryout ini, jadi belum bisa akses pengumuman nilai.") {
  return {
    success: false,
    code: "USER_NOT_ATTEMPTED",
    message,
  };
}

async function deleteRedisKeysByPatterns(patterns = []) {
  const keySet = new Set();

  for (const pattern of patterns) {
    let cursor = "0";
    do {
      const [nextCursor, keys] = await redis.scan(
        cursor,
        "MATCH",
        pattern,
        "COUNT",
        200
      );
      cursor = nextCursor;
      (keys || []).forEach((key) => keySet.add(key));
    } while (cursor !== "0");
  }

  const allKeys = Array.from(keySet);
  if (!allKeys.length) {
    return { deleted: 0 };
  }

  const CHUNK_SIZE = 500;
  let deleted = 0;
  for (let i = 0; i < allKeys.length; i += CHUNK_SIZE) {
    const chunk = allKeys.slice(i, i + CHUNK_SIZE);
    const result = await redis.del(...chunk);
    deleted += Number(result || 0);
  }

  return { deleted };
}

async function buildLatestJawabanTempTable(conn, idTryout) {
  await conn.query(`DROP TEMPORARY TABLE IF EXISTS tmp_latest_jawaban`);
  await conn.query(
    `
    CREATE TEMPORARY TABLE tmp_latest_jawaban AS
    SELECT j.*
    FROM jawaban_user_tryout j
    JOIN (
      SELECT MAX(id) AS max_id
      FROM jawaban_user_tryout
      WHERE id_tryout = ?
      GROUP BY id_user, id_tryout, id_mapel, no_soal
    ) x ON x.max_id = j.id
  `,
    [idTryout]
  );

  await conn.query(
    `
    ALTER TABLE tmp_latest_jawaban
    ADD INDEX idx_lj_mapel_soal (id_mapel, no_soal),
    ADD INDEX idx_lj_user (id_user),
    ADD INDEX idx_lj_status (status)
  `
  );
}

async function autoSetBobotSoalByTryout(conn, idTryout) {
  const [mapels] = await conn.query(
    `
    SELECT id_mapel, COUNT(*) AS total_soal
    FROM soal_tryout
    WHERE id_tryout = ?
    GROUP BY id_mapel
  `,
    [idTryout]
  );

  if (!mapels.length) {
    return { updatedRows: 0, mapelCount: 0 };
  }
  await conn.query(`DROP TEMPORARY TABLE IF EXISTS tmp_soal_stats`);
  await conn.query(
    `
    CREATE TEMPORARY TABLE tmp_soal_stats AS
    SELECT
      st.id_tryout,
      st.id_mapel,
      st.no_soal,
      COALESCE(SUM(CASE WHEN lj.status = 'benar' THEN 1 ELSE 0 END), 0) AS benar,
      COALESCE(SUM(CASE WHEN lj.status IN ('benar', 'salah') THEN 1 ELSE 0 END), 0) AS attempts
    FROM soal_tryout st
    LEFT JOIN tmp_latest_jawaban lj
      ON lj.id_tryout = st.id_tryout
     AND lj.id_mapel = st.id_mapel
     AND lj.no_soal = st.no_soal
    WHERE st.id_tryout = ?
    GROUP BY st.id_tryout, st.id_mapel, st.no_soal
  `,
    [idTryout]
  );
  await conn.query(
    `
    ALTER TABLE tmp_soal_stats
    ADD INDEX idx_tss_mapel_soal (id_mapel, no_soal)
  `
  );

  await conn.query(`DROP TEMPORARY TABLE IF EXISTS tmp_mapel_totals`);
  await conn.query(
    `
    CREATE TEMPORARY TABLE tmp_mapel_totals AS
    SELECT
      id_mapel,
      COUNT(*) AS soal_count,
      AVG(
        CASE
          WHEN attempts <= 0 THEN 0.5
          ELSE 1 - (benar / attempts)
        END
      ) AS avg_raw,
      SUM(
        CASE
          WHEN attempts <= 0 THEN 0.5
          ELSE 1 - (benar / attempts)
        END
      ) AS raw_total
    FROM tmp_soal_stats
    GROUP BY id_mapel
  `
  );
  await conn.query(
    `
    ALTER TABLE tmp_mapel_totals
    ADD PRIMARY KEY (id_mapel)
  `
  );

  const [updateResult] = await conn.query(
    `
    UPDATE soal_tryout st
    JOIN tmp_soal_stats ss
      ON ss.id_tryout = st.id_tryout
     AND ss.id_mapel = st.id_mapel
     AND ss.no_soal = st.no_soal
    JOIN tmp_mapel_totals mt
      ON mt.id_mapel = st.id_mapel
    SET st.point = CASE
      WHEN mt.raw_total > 0 THEN (
        CASE
          WHEN ss.attempts <= 0 THEN 0.5
          ELSE 1 - (ss.benar / ss.attempts)
        END / mt.raw_total
      ) * (
        LEAST(?, GREATEST(?, ? + (0.5 * COALESCE(mt.avg_raw, 0.5))))
      )
      ELSE (
        LEAST(?, GREATEST(?, ? + (0.5 * COALESCE(mt.avg_raw, 0.5)))) / mt.soal_count
      )
    END
    WHERE st.id_tryout = ?
  `,
    [
      AUTO_WEIGHT_MAX_TOTAL,
      AUTO_WEIGHT_MIN_TOTAL,
      AUTO_WEIGHT_TARGET_TOTAL,
      AUTO_WEIGHT_MAX_TOTAL,
      AUTO_WEIGHT_MIN_TOTAL,
      AUTO_WEIGHT_TARGET_TOTAL,
      idTryout
    ]
  );

  return { updatedRows: updateResult.affectedRows || 0, mapelCount: mapels.length };
}

async function shouldSkipAutoWeighting(conn, idTryout) {
  const [rows] = await conn.query(
    `
    SELECT
      COUNT(*) AS total_soal,
      SUM(CASE WHEN point IS NOT NULL AND point > 0 THEN 1 ELSE 0 END) AS soal_berbobot
    FROM soal_tryout
    WHERE id_tryout = ?
  `,
    [idTryout]
  );

  if (!rows.length) {
    return { skip: false, total_soal: 0, soal_berbobot: 0 };
  }

  const totalSoal = Number(rows[0].total_soal || 0);
  const soalBerbobot = Number(rows[0].soal_berbobot || 0);

  return {
    skip: totalSoal > 0 && soalBerbobot === totalSoal,
    total_soal: totalSoal,
    soal_berbobot: soalBerbobot,
  };
}

app.post("/simpan-jawaban-user/:id_tryout", async (req, res) => {
  const startTime = Date.now();
  const conn = await pool.getConnection();
  try {
    const { id_tryout } = req.params;
    await conn.query(`DROP TEMPORARY TABLE IF EXISTS tmp_latest_jawaban_v2`);
    await conn.query(
      `
      CREATE TEMPORARY TABLE tmp_latest_jawaban_v2 AS
      SELECT
        v.id_user,
        v.id_tryout,
        v.id_mapel,
        v.peminatan,
        v.jawaban_user_permapel
      FROM jawaban_user_tryout_v2 v
      JOIN (
        SELECT MAX(id) AS max_id
        FROM jawaban_user_tryout_v2
        WHERE id_tryout = ?
        GROUP BY id_user, id_tryout, id_mapel
      ) x ON x.max_id = v.id
      WHERE v.id_tryout = ?
    `,
      [id_tryout, id_tryout]
    );

    const [[sourceStats]] = await conn.query(
      `
      SELECT
        COUNT(*) AS total_rows,
        SUM(CASE WHEN JSON_VALID(jawaban_user_permapel) = 1 THEN 1 ELSE 0 END) AS valid_json_rows
      FROM tmp_latest_jawaban_v2
    `
    );

    const totalRows = Number(sourceStats?.total_rows || 0);
    const validJsonRows = Number(sourceStats?.valid_json_rows || 0);

    if (totalRows === 0) {
      return res.status(404).json({ success: false, message: "Data tidak ditemukan" });
    }

    if (validJsonRows === 0) {
      return res.status(400).json({ success: false, message: "Tidak ada jawaban valid" });
    }

    await conn.beginTransaction();
    await conn.query(`DELETE FROM jawaban_user_tryout WHERE id_tryout = ?`, [id_tryout]);

    let inserted = 0;
    let mode = "json_table";
    try {
      const [insertResult] = await conn.query(
        `
        INSERT INTO jawaban_user_tryout
        (id_user, id_tryout, id_mapel, no_soal, status, jawaban, peminatan)
        SELECT
          t.id_user,
          t.id_tryout,
          t.id_mapel,
          j.no_soal,
          REPLACE(COALESCE(j.status, ''), '"', '') AS status,
          REPLACE(COALESCE(j.jawaban, ''), '"', '') AS jawaban,
          REPLACE(COALESCE(NULLIF(j.peminatan, ''), t.peminatan, ''), '"', '') AS peminatan
        FROM tmp_latest_jawaban_v2 t
        JOIN JSON_TABLE(
          t.jawaban_user_permapel,
          '$[*]' COLUMNS (
            no_soal INT PATH '$.no_soal' NULL ON EMPTY NULL ON ERROR,
            status VARCHAR(20) PATH '$.status' NULL ON EMPTY NULL ON ERROR,
            jawaban VARCHAR(255) PATH '$.jawaban' NULL ON EMPTY NULL ON ERROR,
            peminatan VARCHAR(50) PATH '$.peminatan' NULL ON EMPTY NULL ON ERROR
          )
        ) AS j
        WHERE JSON_VALID(t.jawaban_user_permapel) = 1
          AND j.no_soal IS NOT NULL
      `
      );
      inserted = insertResult.affectedRows || 0;
    } catch (insertErr) {
      // Fallback untuk engine DB yang belum support JSON_TABLE.
      mode = "js_fallback";
      const fallbackableCodes = new Set(["ER_PARSE_ERROR", "ER_NOT_SUPPORTED_YET"]);
      if (!fallbackableCodes.has(insertErr?.code)) {
        throw insertErr;
      }

      const [fallbackRows] = await conn.query(
        `
        SELECT id_user, id_tryout, id_mapel, peminatan, jawaban_user_permapel
        FROM tmp_latest_jawaban_v2
        WHERE JSON_VALID(jawaban_user_permapel) = 1
      `
      );

      const values = [];
      fallbackRows.forEach((row) => {
        try {
          const parsed = JSON.parse(row.jawaban_user_permapel);
          if (!Array.isArray(parsed)) {
            return;
          }
          parsed.forEach((item) => {
            const noSoal = Number(item?.no_soal);
            if (!Number.isFinite(noSoal)) {
              return;
            }
            values.push([
              row.id_user,
              row.id_tryout,
              row.id_mapel,
              noSoal,
              (item?.status || "").toString().replace(/"/g, ""),
              (item?.jawaban || "").toString().replace(/"/g, ""),
              (item?.peminatan || row.peminatan || "").toString().replace(/"/g, "")
            ]);
          });
        } catch (e) {
          // skip json invalid per baris
        }
      });

      if (!values.length) {
        throw new Error("Tidak ada jawaban valid untuk disimpan");
      }

      const insertSql = `
        INSERT INTO jawaban_user_tryout
        (id_user, id_tryout, id_mapel, no_soal, status, jawaban, peminatan)
        VALUES ?
      `;
      const CHUNK_SIZE = 3000;
      for (let i = 0; i < values.length; i += CHUNK_SIZE) {
        const chunk = values.slice(i, i + CHUNK_SIZE);
        const [chunkResult] = await conn.query(insertSql, [chunk]);
        inserted += chunkResult.affectedRows || 0;
      }
    }

    await conn.commit();

    res.json({
      success: true,
      inserted,
      mode,
      source_rows: totalRows,
      valid_json_rows: validJsonRows,
      timing_ms: Date.now() - startTime
    });
  } catch (err) {
    try {
      await conn.rollback();
    } catch (rollbackErr) {
      console.error("Rollback error:", rollbackErr);
    }
    console.error("Bulk insert error:", err);
    res.status(500).json({ success: false, error: err.message });
  } finally {
    conn.release();
  }
});

// 🚀 API untuk generate ranking & simpan ke rank_tryout_2025
app.post("/process-tryout", async (req, res) => {
  const { idTryout,jenis } = req.body;
  const conn = await pool.getConnection();
  const processStart = Date.now();
  const normalizedJenis = normalizeJenis(jenis);
  const isKedinasan = isKedinasanJenis(normalizedJenis);
  const isUmUgm = isUmUgmJenis(normalizedJenis);
  const timings = {};
  const mark = (name, startAt) => {
    timings[name] = Date.now() - startAt;
  };

  try {
    let stepStart = Date.now();
    await conn.beginTransaction();
    mark("begin_transaction_ms", stepStart);

    stepStart = Date.now();
    await buildLatestJawabanTempTable(conn, idTryout);
    mark("build_tmp_latest_jawaban_ms", stepStart);

    // 0. Auto set bobot soal berbasis performa user (benar/salah).
    // Bobot per mapel dinormalisasi pada total 9-10 (set default 9.5).
    stepStart = Date.now();
    const weightGuard = await shouldSkipAutoWeighting(conn, idTryout);
    let weightSummary = {
      updatedRows: 0,
      mapelCount: 0,
      skipped: false,
      reason: null,
      guard: weightGuard,
    };

    if (isKedinasan) {
      weightSummary = {
        ...weightSummary,
        skipped: true,
        reason: "not_required_for_kedinasan",
      };
    } else if (!weightGuard.skip) {
      const autoWeightResult = await autoSetBobotSoalByTryout(conn, idTryout);
      weightSummary = {
        ...weightSummary,
        ...autoWeightResult,
      };
    } else {
      weightSummary = {
        ...weightSummary,
        skipped: true,
        reason: "all_soal_already_weighted",
      };
    }
    mark("auto_weighting_ms", stepStart);

    // 1. Hapus ranking lama biar tidak dobel
    stepStart = Date.now();
    await conn.query(
      `DELETE FROM rank_tryout_2025 WHERE id_tryout = ?`,
      [idTryout]
    );
    mark("delete_old_rank_ms", stepStart);

    // 2. Hitung nilai + ranking
    stepStart = Date.now();
    if (isKedinasan) {
      // Kedinasan: TWK/TIU = benar*5, TKP(id_mapel=69) = skor TKP langsung (kolom benar)
      await conn.query(
        `
        INSERT INTO rank_tryout_2025
        (id_user, username, peminatan, total, instansi, provinsi, \`rank\`, id_tryout, year)
        SELECT
          r.id_user,
          r.username,
          r.peminatan,
          r.total,
          ud.instansi,
          ud.provinsi,
          r.rnk,
          ?,
          2026
        FROM (
          SELECT
            n.id_user,
            n.username,
            n.peminatan,
            n.total,
            ROW_NUMBER() OVER (ORDER BY n.total DESC) AS rnk
          FROM (
            SELECT
              v.id_user,
              u.username,
              COALESCE(MAX(NULLIF(v.peminatan, '')), 'ipc') AS peminatan,
              SUM(
                CASE
                  WHEN v.id_mapel = 69 THEN COALESCE(v.benar, 0)
                  ELSE COALESCE(v.benar, 0) * 5
                END
              ) AS total
            FROM jawaban_user_tryout_v2 v
            JOIN (
              SELECT MAX(id) AS max_id
              FROM jawaban_user_tryout_v2
              WHERE id_tryout = ?
              GROUP BY id_user, id_tryout, id_mapel
            ) x ON x.max_id = v.id
            JOIN users u ON u.id = v.id_user
            GROUP BY v.id_user, u.username
          ) n
        ) r
        LEFT JOIN userdata ud ON ud.id_user = r.id_user;
        `,
        [idTryout, idTryout]
      );
    } else if (isUmUgm) {
      await conn.query(
        `
        INSERT INTO rank_tryout_2025
        (id_user, username, peminatan, total, instansi, provinsi, \`rank\`, id_tryout, year)
        SELECT
          r.id_user,
          r.username,
          r.peminatan,
          r.total,
          ud.instansi,
          ud.provinsi,
          r.rnk,
          ?,
          2026
        FROM (
          SELECT
            n.id_user,
            n.username,
            n.peminatan,
            n.total,
            ROW_NUMBER() OVER (ORDER BY n.total DESC) AS rnk
          FROM (
            SELECT
              jut.id_user,
              u.username,
              COALESCE(MAX(NULLIF(jut.peminatan, '')), 'Saintek') AS peminatan,
              (
                SUM(
                  CASE
                    WHEN (jut.id_mapel = 51 OR UPPER(TRIM(mp.nama)) = 'TPA') THEN
                      CASE
                        WHEN jut.status = 'benar' THEN 1
                        ELSE 0
                      END
                    ELSE
                      CASE
                        WHEN jut.status = 'benar' THEN 4
                        WHEN jut.status = 'salah' THEN -1
                        ELSE 0
                      END
                  END
                ) / 360
              ) * 1000 AS total
            FROM tmp_latest_jawaban jut
            JOIN soal_tryout st
              ON st.no_soal = jut.no_soal
             AND st.id_mapel = jut.id_mapel
             AND st.id_tryout = jut.id_tryout
            JOIN mata_pelajaran mp ON mp.id = jut.id_mapel
            JOIN users u ON u.id = jut.id_user
            GROUP BY jut.id_user, u.username
          ) n
        ) r
        LEFT JOIN userdata ud ON ud.id_user = r.id_user;
        `,
        [idTryout]
      );
    } else {
      await conn.query(
        `
        INSERT INTO rank_tryout_2025
        (id_user, username, peminatan, total, instansi, provinsi, \`rank\`, id_tryout, year)
        SELECT
          r.id_user,
          r.username,
          r.peminatan,
          r.total,
          ud.instansi,
          ud.provinsi,
          r.rnk,
          ?,
          2026
        FROM (
          SELECT
            n.id_user,
            n.username,
            n.peminatan,
            n.total,
            ROW_NUMBER() OVER (ORDER BY n.total DESC) AS rnk
          FROM (
            SELECT
              jut.id_user,
              u.username,
              COALESCE(MAX(NULLIF(jut.peminatan, '')), 'Saintek') AS peminatan,
              (
                SUM(
                  CASE
                    WHEN jut.status = 'benar' THEN
                      CASE
                        WHEN ? = 'tka' THEN 5
                        ELSE st.point * 100
                      END
                    ELSE 0
                  END
                )
              ) / (
                CASE
                  WHEN ? = 'tka' THEN 1
                  WHEN ? = 'umptkin' THEN NULLIF(COUNT(DISTINCT jut.id_mapel), 0)
                  ELSE 7
                END
              ) AS total
            FROM tmp_latest_jawaban jut
            JOIN soal_tryout st
              ON st.no_soal = jut.no_soal
             AND st.id_mapel = jut.id_mapel
             AND st.id_tryout = jut.id_tryout
            JOIN users u ON u.id = jut.id_user
            GROUP BY jut.id_user, u.username
          ) n
        ) r
        LEFT JOIN userdata ud ON ud.id_user = r.id_user;
        `,
        [idTryout, normalizedJenis, normalizedJenis, normalizedJenis]
      );
    }
    mark("insert_rank_ms", stepStart);


    stepStart = Date.now();
    await conn.query(
      `delete
      FROM jawaban_user_tryout_pembahasan 
      WHERE id_tryout = ?
    `,
      [idTryout]
    );
    mark("delete_pembahasan_ms", stepStart);
    
    // 3. Copy jawaban user ke tabel pembahasan
    stepStart = Date.now();
    await conn.query(
      `
      INSERT INTO jawaban_user_tryout_pembahasan 
          (id_user,id_tryout,id_mapel,no_soal, status, jawaban,peminatan)
      SELECT id_user,id_tryout,id_mapel,no_soal, status,jawaban,peminatan
      FROM tmp_latest_jawaban
    `,
    );
    mark("insert_pembahasan_ms", stepStart);

    stepStart = Date.now();
     await conn.query(
      `delete
      FROM jawaban_user_tryout_pembahasan_v2 
      WHERE id_tryout = ?
    `,
      [idTryout]
    );
    mark("delete_pembahasan_v2_ms", stepStart);

    stepStart = Date.now();
    await conn.query(
      `
      INSERT INTO jawaban_user_tryout_pembahasan_v2 
          (id,id_user,id_tryout,id_mapel,jawaban_user_permapel,peminatan,kosong,salah,benar)
      SELECT v.id,v.id_user,v.id_tryout,v.id_mapel,v.jawaban_user_permapel,v.peminatan,v.kosong,v.salah,v.benar
      FROM jawaban_user_tryout_v2 v
      JOIN (
        SELECT MAX(id) AS max_id
        FROM jawaban_user_tryout_v2
        WHERE id_tryout = ?
        GROUP BY id_user, id_tryout, id_mapel
      ) x ON x.max_id = v.id
    `,
      [idTryout]
    );
    mark("insert_pembahasan_v2_ms", stepStart);

    stepStart = Date.now();
    await conn.commit();
    mark("commit_ms", stepStart);

    stepStart = Date.now();
    await redis.flushdb("ASYNC");
    mark("redis_flushdb_async_call_ms", stepStart);

    timings.total_process_ms = Date.now() - processStart;
    res.json({
      success: true,
      message: `Ranking & pembahasan berhasil diproses untuk tryout ${idTryout}`,
      auto_weighting: {
        mapel_processed: weightSummary.mapelCount,
        soal_updated: weightSummary.updatedRows,
        total_range: [AUTO_WEIGHT_MIN_TOTAL, AUTO_WEIGHT_MAX_TOTAL],
        target_total: AUTO_WEIGHT_TARGET_TOTAL,
        skipped: weightSummary.skipped,
        reason: weightSummary.reason,
        guard: weightSummary.guard,
      },
      timings,
    });
  } catch (err) {
    await conn.rollback();
    console.error(err);
    res.status(500).json({ success: false, error: err.message });
  } finally {
    conn.release();
  }
});

// 🚀 API untuk proses pengumuman per-user (versi ringan untuk halaman user)
app.post("/process-tryout-user", async (req, res) => {
  const { idTryout, idUser, jenis } = req.body;
  const normalizedJenis = normalizeJenis(jenis);
  const isKedinasan = isKedinasanJenis(normalizedJenis);
  const isUmUgm = isUmUgmJenis(normalizedJenis);
  const requestMeta = {
    idTryout,
    idUser,
    jenis: normalizedJenis || jenis || "",
    ip: req.ip,
  };

  if (!idTryout || !idUser) {
    logProcessTryoutUser("warn", "invalid request body", {
      ...requestMeta,
      bodyKeys: Object.keys(req.body || {}),
    });
    return res.status(400).json({
      success: false,
      message: "idTryout dan idUser wajib diisi",
    });
  }

  const conn = await pool.getConnection();
  const processStart = Date.now();
  const timings = {};
  const mark = (name, startAt) => {
    timings[name] = Date.now() - startAt;
  };

  try {
    let stepStart = Date.now();
    await conn.beginTransaction();
    mark("begin_transaction_ms", stepStart);

    // 1) Ambil jawaban terbaru per-mapel untuk user ini dari tabel v2
    stepStart = Date.now();
    const [latestV2Rows] = await conn.query(
      `
      SELECT v.*
      FROM jawaban_user_tryout_v2 v
      JOIN (
        SELECT MAX(id) AS max_id
        FROM jawaban_user_tryout_v2
        WHERE id_tryout = ? AND id_user = ?
        GROUP BY id_user, id_tryout, id_mapel
      ) x ON x.max_id = v.id
      `,
      [idTryout, idUser]
    );
    mark("fetch_latest_v2_ms", stepStart);

    if (!latestV2Rows.length) {
      await conn.rollback();
      logProcessTryoutUser("warn", "latest v2 not found", requestMeta);
      return res.status(200).json(userNotAttemptedResponse());
    }

    // 2) Parse JSON jawaban per-mapel jadi bentuk jawaban detail
    stepStart = Date.now();
    const parsedJawaban = [];
    const invalidJsonRows = [];
    latestV2Rows.forEach((row) => {
      try {
        const arr = JSON.parse(row.jawaban_user_permapel || "[]");
        if (!Array.isArray(arr)) return;
        arr.forEach((item) => {
          parsedJawaban.push([
            Number(idUser),
            Number(idTryout),
            Number(item.id_mapel),
            Number(item.no_soal),
            (item.status || "").toString().replace(/"/g, ""),
            (item.jawaban || "").toString().replace(/"/g, ""),
            (item.peminatan || row.peminatan || "").toString().replace(/"/g, ""),
          ]);
        });
      } catch (e) {
        invalidJsonRows.push({
          id_mapel: row.id_mapel,
          row_id: row.id,
          message: e.message,
        });
      }
    });
    mark("parse_jawaban_ms", stepStart);

    if (!parsedJawaban.length) {
      await conn.rollback();
      logProcessTryoutUser("warn", "parsed jawaban empty", {
        ...requestMeta,
        latestV2Count: latestV2Rows.length,
        invalidJsonCount: invalidJsonRows.length,
        invalidJsonRows,
      });
      if (invalidJsonRows.length > 0) {
        return res.status(400).json({
          success: false,
          code: "INVALID_JAWABAN_DATA",
          message: "Data jawaban tryout tidak valid. Silakan hubungi admin.",
        });
      }
      return res.status(200).json(userNotAttemptedResponse());
    }

    // 3) Refresh tabel jawaban detail untuk user ini saja
    stepStart = Date.now();
    await conn.query(
      `DELETE FROM jawaban_user_tryout WHERE id_tryout = ? AND id_user = ?`,
      [idTryout, idUser]
    );
    await conn.query(
      `
      INSERT INTO jawaban_user_tryout
      (id_user, id_tryout, id_mapel, no_soal, status, jawaban, peminatan)
      VALUES ?
      `,
      [parsedJawaban]
    );
    mark("refresh_jawaban_detail_ms", stepStart);

    // 4) Refresh tabel pembahasan detail untuk user ini saja
    stepStart = Date.now();
    await conn.query(
      `DELETE FROM jawaban_user_tryout_pembahasan WHERE id_tryout = ? AND id_user = ?`,
      [idTryout, idUser]
    );
    await conn.query(
      `
      INSERT INTO jawaban_user_tryout_pembahasan
      (id_user, id_tryout, id_mapel, no_soal, status, jawaban, peminatan)
      VALUES ?
      `,
      [parsedJawaban]
    );
    mark("refresh_pembahasan_ms", stepStart);

    // 5) Refresh pembahasan_v2 untuk user ini saja
    stepStart = Date.now();
    await conn.query(
      `DELETE FROM jawaban_user_tryout_pembahasan_v2 WHERE id_tryout = ? AND id_user = ?`,
      [idTryout, idUser]
    );
    await conn.query(
      `
      INSERT INTO jawaban_user_tryout_pembahasan_v2
      (id, id_user, id_tryout, id_mapel, jawaban_user_permapel, peminatan, kosong, salah, benar)
      SELECT id, id_user, id_tryout, id_mapel, jawaban_user_permapel, peminatan, kosong, salah, benar
      FROM jawaban_user_tryout_v2
      WHERE id IN (${latestV2Rows.map(() => "?").join(",")})
      `,
      latestV2Rows.map((x) => x.id)
    );
    mark("refresh_pembahasan_v2_ms", stepStart);

    // 6) Hitung total user (tanpa proses massal semua peserta)
    stepStart = Date.now();
    const [scoreRows] = isKedinasan
      ? await conn.query(
          `
          SELECT
            v.id_user,
            COALESCE(SUM(
              CASE
                WHEN v.id_mapel = 69 THEN COALESCE(v.benar, 0)
                ELSE COALESCE(v.benar, 0) * 5
              END
            ), 0) AS raw_total,
            COALESCE(MAX(NULLIF(v.peminatan, '')), 'ipc') AS peminatan
          FROM jawaban_user_tryout_v2 v
          JOIN (
            SELECT MAX(id) AS max_id
            FROM jawaban_user_tryout_v2
            WHERE id_tryout = ? AND id_user = ?
            GROUP BY id_user, id_tryout, id_mapel
          ) x ON x.max_id = v.id
          GROUP BY v.id_user
          `,
          [idTryout, idUser]
        )
      : await conn.query(
          `
          SELECT
            ju.id_user,
            COALESCE(SUM(
              CASE
                WHEN ? = 'um ugm' THEN
                  CASE
                    WHEN (ju.id_mapel = 51 OR UPPER(TRIM(mp.nama)) = 'TPA') THEN
                      CASE
                        WHEN ju.status = 'benar' THEN 1
                        ELSE 0
                      END
                    ELSE
                      CASE
                        WHEN ju.status = 'benar' THEN 4
                        WHEN ju.status = 'salah' THEN -1
                        ELSE 0
                      END
                  END
                WHEN ju.status = 'benar' THEN
                  CASE
                    WHEN ? = 'tka' THEN 5
                    ELSE st.point * 100
                  END
                ELSE 0
              END
            ), 0) AS raw_total,
            COALESCE(MAX(NULLIF(ju.peminatan, '')), 'Saintek') AS peminatan
          FROM jawaban_user_tryout ju
          JOIN soal_tryout st
            ON st.no_soal = ju.no_soal
           AND st.id_mapel = ju.id_mapel
           AND st.id_tryout = ju.id_tryout
          JOIN mata_pelajaran mp ON mp.id = ju.id_mapel
          WHERE ju.id_tryout = ? AND ju.id_user = ?
          GROUP BY ju.id_user
          `,
          [normalizedJenis, normalizedJenis, idTryout, idUser]
        );
    mark("calculate_score_ms", stepStart);

    if (!scoreRows.length) {
      await conn.rollback();
      logProcessTryoutUser("warn", "score rows empty", {
        ...requestMeta,
        parsedJawabanCount: parsedJawaban.length,
      });
      return res.status(200).json(userNotAttemptedResponse());
    }

    const rawTotal = Number(scoreRows[0].raw_total || 0);
    const userPeminatan = scoreRows[0].peminatan || (isKedinasan ? "ipc" : "Saintek");
    const finalTotal = isUmUgm
      ? (rawTotal / 360) * 1000
      : (normalizedJenis === "tka" || isKedinasan
        ? rawTotal
        : rawTotal / 7);

    // 7) Upsert ranking user ke rank_tryout_2025
    stepStart = Date.now();
    const [userRows] = await conn.query(
      `
      SELECT u.id, u.username, ud.instansi, ud.provinsi
      FROM users u
      LEFT JOIN userdata ud ON ud.id_user = u.id
      WHERE u.id = ?
      LIMIT 1
      `,
      [idUser]
    );
    if (!userRows.length) {
      await conn.rollback();
      logProcessTryoutUser("warn", "user not found", requestMeta);
      return res.status(404).json({
        success: false,
        message: "Data user tidak ditemukan",
      });
    }

    await conn.query(
      `DELETE FROM rank_tryout_2025 WHERE id_tryout = ? AND id_user = ?`,
      [idTryout, idUser]
    );
    await conn.query(
      `
      INSERT INTO rank_tryout_2025
      (id_user, username, peminatan, total, instansi, provinsi, \`rank\`, id_tryout, year)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, 2026)
      `,
      [
        Number(idUser),
        userRows[0].username || "",
        userPeminatan,
        finalTotal,
        userRows[0].instansi || null,
        userRows[0].provinsi || null,
        0,
        Number(idTryout),
      ]
    );
    mark("upsert_rank_ms", stepStart);

    // 8) Hitung rank user berdasarkan data rank yang sudah ada
    stepStart = Date.now();
    const [rankRows] = await conn.query(
      `
      SELECT COUNT(*) + 1 AS rank_position
      FROM rank_tryout_2025
      WHERE id_tryout = ? AND total > ?
      `,
      [idTryout, finalTotal]
    );
    const userRank = Number(rankRows?.[0]?.rank_position || 1);

    await conn.query(
      `UPDATE rank_tryout_2025 SET \`rank\` = ? WHERE id_tryout = ? AND id_user = ?`,
      [userRank, idTryout, idUser]
    );
    mark("update_user_rank_ms", stepStart);

    stepStart = Date.now();
    await conn.commit();
    mark("commit_ms", stepStart);

    // 9) Invalidate cache yang terkait halaman pengumuman user ini saja
    stepStart = Date.now();
    const cacheClear = await deleteRedisKeysByPatterns([
      `nilaitosaintek_${idTryout}_${idUser}_*`,
      `nilaitososhum_${idTryout}_${idUser}_*`,
      `nilaitoipc_${idTryout}_${idUser}_*`,
      `status_pengumuman_to_${idTryout}_${idUser}_*`,
      `check_rank_per_tryout_${idTryout}_${idUser}*`,
    ]);
    mark("clear_related_cache_ms", stepStart);

    timings.total_process_ms = Date.now() - processStart;
    logProcessTryoutUser("info", "process success", {
      ...requestMeta,
      total: finalTotal,
      rank: userRank,
      timings,
    });
    return res.json({
      success: true,
      message: `Pengumuman user ${idUser} untuk tryout ${idTryout} berhasil diproses`,
      data: {
        idTryout: Number(idTryout),
        idUser: Number(idUser),
        total: finalTotal,
        rank: userRank,
        deleted_cache_keys: cacheClear.deleted,
      },
      timings,
    });
  } catch (err) {
    try {
      await conn.rollback();
    } catch (rollbackErr) {
      console.error("Rollback error:", rollbackErr);
    }
    logProcessTryoutUser("error", "process failed", {
      ...requestMeta,
      error: err.message,
      stack: err.stack,
    });
    return res.status(500).json({ success: false, error: err.message });
  } finally {
    conn.release();
  }
});


app.post("/delete-jawaban-pembahasan", async (req, res) => {
  const { idTryout } = req.body;
  const conn = await pool.getConnection();

  try {
    await conn.beginTransaction();

    await conn.query(
      `DELETE FROM jawaban_user_tryout_pembahasan WHERE id_tryout = ?`,
      [idTryout]
    );

    await conn.commit();
    res.json({ success: true, message: `Data pembahasan untuk tryout ${idTryout} berhasil dihapus` });
  } catch (err) {
    await conn.rollback();
    console.error(err);
    res.status(500).json({ success: false, error: err.message });
  } finally {
    conn.release();
  }
});


app.post("/copy-jawaban-pembahasan", async (req, res) => {
  const { idTryout } = req.body;
  const conn = await pool.getConnection();

  try {
    await conn.beginTransaction();

    await conn.query(
      `
      INSERT INTO jawaban_user_tryout_pembahasan 
          (id_user,id_tryout,id_mapel,no_soal, status, jawaban,peminatan)
      SELECT id_user,id_tryout,id_mapel,no_soal, status,jawaban,peminatan
      FROM jawaban_user_tryout 
      WHERE id_tryout = ?
      `,
      [idTryout]
    );

    await conn.commit();
    res.json({ success: true, message: `Data pembahasan untuk tryout ${idTryout} berhasil dicopy` });
  } catch (err) {
    await conn.rollback();
    console.error(err);
    res.status(500).json({ success: false, error: err.message });
  } finally {
    conn.release();
  }
});


// 🚀 API untuk ambil ranking hasil
app.get("/ranking/:idTryout", async (req, res) => {
  const { idTryout } = req.params;

  try {
    const [rows] = await pool.query(
      `
      SELECT 
          r.id_user,
          u.username,
          r.peminatan,
          COALESCE(r.total, 0) AS total,
          COALESCE(r.instansi, '-') AS instansi,
          COALESCE(r.provinsi, 0) AS provinsi,
          r.rank,
          p.province_name,
          u.image
      FROM rank_tryout_2025 r
      JOIN users u ON u.id = r.id_user
      LEFT JOIN province p ON p.id = r.provinsi
      WHERE r.id_tryout = ?
      ORDER BY r.total DESC
    `,
      [idTryout]
    );

    res.json({ success: true, data: rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// jalankan server
const server = app.listen(2234, () => {
  console.log("Server pengumuman running on http://localhost:2234");
});

server.timeout = 600000; // untuk request
server.keepAliveTimeout = 620000; // jaga supaya koneksi gak putus duluan
