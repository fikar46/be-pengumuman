import express from "express";
import mysql from "mysql2/promise";
import Redis from "ioredis";
import http from "http";
import {
  buildTkaSubjectResult,
  hasTkaIstimewaPredicate,
  toTkaScaledScoreFromTotal,
} from "./tkaScoring.js";

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
const NIGHTLY_SCHEDULER_INTERVAL_MS = 30 * 1000;
const NIGHTLY_SCHEDULER_TIME_ZONE = "Asia/Jakarta";

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

function isSimakUiJenis(jenis = "") {
  return normalizeJenis(jenis) === "simak ui";
}

function isUmUnsJenis(jenis = "") {
  return normalizeJenis(jenis) === "um uns";
}

async function resolveJenisTryout(conn, idTryout, providedJenis = "") {
  const normalizedProvided = normalizeJenis(providedJenis);
  if (normalizedProvided) {
    return normalizedProvided;
  }

  if (!conn || !idTryout) {
    return "";
  }

  try {
    const [rows] = await conn.query(
      `SELECT tipe_tryout FROM tryout WHERE id = ? LIMIT 1`,
      [idTryout]
    );
    return normalizeJenis(rows?.[0]?.tipe_tryout || "");
  } catch (error) {
    console.warn("[resolveJenisTryout] failed to resolve tipe_tryout", {
      idTryout,
      message: error.message,
    });
    return "";
  }
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

function getJakartaDateParts(date = new Date()) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: NIGHTLY_SCHEDULER_TIME_ZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
  }).formatToParts(date).reduce((result, item) => {
    result[item.type] = item.value;
    return result;
  }, {});
  return {
    date: `${parts.year}-${parts.month}-${parts.day}`,
    hour: Number(parts.hour),
    minute: Number(parts.minute),
  };
}

function addCalendarDays(dateValue, days) {
  const date = new Date(`${dateValue}T00:00:00Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

async function ensureNightlySchedulerTable(conn) {
  await conn.query(`
    CREATE TABLE IF NOT EXISTS tryout_nightly_process_log (
      id BIGINT NOT NULL AUTO_INCREMENT,
      id_tryout BIGINT NOT NULL,
      process_date DATE NOT NULL,
      status VARCHAR(20) NOT NULL DEFAULT 'running',
      started_at DATETIME NOT NULL,
      finished_at DATETIME NULL,
      error_message TEXT NULL,
      PRIMARY KEY (id),
      UNIQUE KEY uniq_tryout_process_date (id_tryout, process_date),
      INDEX idx_nightly_status (status, process_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `);
}

function postLocalPengumuman(path, payload = {}, timeoutMs = 15 * 60 * 1000) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify(payload);
    const request = http.request({
      hostname: "127.0.0.1",
      port: 2234,
      path,
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(body),
      },
      timeout: timeoutMs,
    }, (response) => {
      let responseBody = "";
      response.setEncoding("utf8");
      response.on("data", (chunk) => { responseBody += chunk; });
      response.on("end", () => {
        let parsed = {};
        try { parsed = responseBody ? JSON.parse(responseBody) : {}; } catch (error) {}
        if (response.statusCode >= 200 && response.statusCode < 300) {
          resolve(parsed);
          return;
        }
        reject(new Error(parsed.message || parsed.error || `HTTP ${response.statusCode}`));
      });
    });
    request.on("timeout", () => request.destroy(new Error("Nightly process timeout")));
    request.on("error", reject);
    request.write(body);
    request.end();
  });
}

let nightlySchedulerRunning = false;
async function runNightlyTryoutScheduler() {
  const jakartaNow = getJakartaDateParts();
  if (nightlySchedulerRunning) {
    return;
  }
  const isNightlyWindow = jakartaNow.hour === 23 && jakartaNow.minute === 59;

  nightlySchedulerRunning = true;
  const conn = await pool.getConnection();
  try {
    await ensureNightlySchedulerTable(conn);
    const [tryoutRows] = await conn.query(
      `
      SELECT id,
        DATE_FORMAT(DATE(start_time), '%Y-%m-%d') AS first_process_date,
        DATE_FORMAT(DATE_ADD(DATE(end_time), INTERVAL 1 DAY), '%Y-%m-%d') AS final_process_date
      FROM tryout
      WHERE (LOWER(COALESCE(jenis, '')) IN ('sbmptn', 'snbt')
             OR LOWER(COALESCE(tipe_tryout, '')) IN ('sbmptn', 'snbt'))
        AND DATE(start_time) <= ?
        AND DATE_ADD(DATE(end_time), INTERVAL 1 DAY) >= ?
      `,
      [jakartaNow.date, jakartaNow.date]
    );

    for (const tryout of tryoutRows) {
      const firstProcessDate = tryout.first_process_date;
      const finalProcessDate = tryout.final_process_date;
      if (!firstProcessDate || !finalProcessDate || firstProcessDate > jakartaNow.date) {
        continue;
      }
      const lastDueDate = finalProcessDate < jakartaNow.date ? finalProcessDate : jakartaNow.date;
      if (lastDueDate < firstProcessDate) {
        continue;
      }

      const [processedRows] = await conn.query(
        `SELECT DATE_FORMAT(process_date, '%Y-%m-%d') AS process_date, status FROM tryout_nightly_process_log
         WHERE id_tryout = ? AND process_date BETWEEN ? AND ?`,
        [tryout.id, firstProcessDate, lastDueDate]
      );
      const processedSuccessDates = new Set(
        processedRows.filter((row) => row.status === "success").map((row) => String(row.process_date).slice(0, 10))
      );
      let processDate = firstProcessDate;
      while (processDate <= lastDueDate && processedSuccessDates.has(processDate)) {
        processDate = addCalendarDays(processDate, 1);
      }
      if (processDate > lastDueDate || (processDate === jakartaNow.date && !isNightlyWindow)) {
        continue;
      }

      const lockName = `nightly_tryout_${tryout.id}_${processDate}`;
      const [lockRows] = await conn.query(`SELECT GET_LOCK(?, 0) AS acquired`, [lockName]);
      if (Number(lockRows?.[0]?.acquired || 0) !== 1) {
        continue;
      }

      try {
        const [existingRows] = await conn.query(
          `SELECT status FROM tryout_nightly_process_log WHERE id_tryout = ? AND process_date = ? LIMIT 1`,
          [tryout.id, processDate]
        );
        if (existingRows.some((row) => row.status === "success")) {
          continue;
        }

        await conn.query(
          `
          INSERT INTO tryout_nightly_process_log (id_tryout, process_date, status, started_at, finished_at, error_message)
          VALUES (?, ?, 'running', NOW(), NULL, NULL)
          ON DUPLICATE KEY UPDATE status = 'running', started_at = NOW(), finished_at = NULL, error_message = NULL
          `,
          [tryout.id, processDate]
        );

        try {
          await postLocalPengumuman(`/simpan-jawaban-user/${tryout.id}`, {});
          await postLocalPengumuman("/process-tryout", {
            idTryout: tryout.id,
            jenis: "sbmptn",
            forceReweight: true,
            schedulerProcessDate: processDate,
          });
          await conn.query(
            `UPDATE tryout_nightly_process_log SET status = 'success', finished_at = NOW() WHERE id_tryout = ? AND process_date = ?`,
            [tryout.id, processDate]
          );
          console.log("[nightly-scheduler] success", { idTryout: tryout.id, processDate, catchUp: processDate !== jakartaNow.date });
        } catch (error) {
          await conn.query(
            `UPDATE tryout_nightly_process_log SET status = 'failed', finished_at = NOW(), error_message = ? WHERE id_tryout = ? AND process_date = ?`,
            [String(error.message || error).slice(0, 4000), tryout.id, processDate]
          );
          console.error("[nightly-scheduler] failed", { idTryout: tryout.id, processDate, message: error.message });
        }
      } finally {
        await conn.query(`SELECT RELEASE_LOCK(?)`, [lockName]);
      }
    }
  } catch (error) {
    console.error("[nightly-scheduler] tick failed", { message: error.message });
  } finally {
    conn.release();
    nightlySchedulerRunning = false;
  }
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

// Formula ini sengaja disamakan dengan tombol "Simpan Semua Point" di admin:
// (1 - benar / (benar + salah)) / (jumlah soal yang muncul / 10).
async function saveAdminStylePointsByTryout(conn, idTryout) {
  const [rows] = await conn.query(
    `
    SELECT
      st.id_mapel,
      st.no_soal,
      SUM(CASE WHEN jut.status = 'benar' THEN 1 ELSE 0 END) AS benar,
      SUM(CASE WHEN jut.status = 'salah' THEN 1 ELSE 0 END) AS salah
    FROM jawaban_user_tryout jut
    JOIN soal_tryout st
      ON st.id_tryout = jut.id_tryout
     AND st.id_mapel = jut.id_mapel
     AND st.no_soal = jut.no_soal
    WHERE jut.id_tryout = ?
    GROUP BY st.id_mapel, st.no_soal
    ORDER BY st.id_mapel, st.no_soal
    `,
    [idTryout]
  );

  const rowsByMapel = new Map();
  rows.forEach((row) => {
    const idMapel = Number(row.id_mapel);
    if (!rowsByMapel.has(idMapel)) rowsByMapel.set(idMapel, []);
    rowsByMapel.get(idMapel).push(row);
  });

  let updatedRows = 0;
  rowsByMapel.forEach((mapelRows) => {
    mapelRows.forEach((row) => {
      const benar = Number(row.benar || 0);
      const salah = Number(row.salah || 0);
      const jumlah = benar + salah;
      if (!jumlah) return;
      const score = (1 - (benar / jumlah)) / (mapelRows.length / 10);
      row.__score = score;
    });
  });

  for (const row of rows) {
    if (row.__score === undefined) continue;
    const [result] = await conn.query(
      `UPDATE soal_tryout SET point = ? WHERE id_tryout = ? AND id_mapel = ? AND no_soal = ?`,
      [row.__score, idTryout, row.id_mapel, row.no_soal]
    );
    updatedRows += result.affectedRows || 0;
  }

  return { updatedRows, mapelCount: rowsByMapel.size };
}

async function shouldSkipAutoWeighting(conn, idTryout) {
  const [rows] = await conn.query(
    `
    SELECT
      COUNT(*) AS total_soal,
      SUM(CASE WHEN point IS NOT NULL THEN 1 ELSE 0 END) AS soal_berbobot,
      SUM(COALESCE(point, 0)) AS total_point
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
  const totalPoint = Number(rows[0].total_point || 0);

  return {
    skip: totalSoal > 0 && soalBerbobot === totalSoal && totalPoint > 0,
    total_soal: totalSoal,
    soal_berbobot: soalBerbobot,
    total_point: totalPoint,
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
  const { idTryout, jenis, tipe_tryout, forceReweight = false, schedulerProcessDate } = req.body;
  const conn = await pool.getConnection();
  const processStart = Date.now();
  const normalizedJenis = await resolveJenisTryout(conn, idTryout, jenis || tipe_tryout);
  const isKedinasan = isKedinasanJenis(normalizedJenis);
  const isUmUgm = isUmUgmJenis(normalizedJenis);
  const isSimakUi = isSimakUiJenis(normalizedJenis);
  const isUmUns = isUmUnsJenis(normalizedJenis);
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
    } else if (isSimakUi) {
      weightSummary = {
        ...weightSummary,
        skipped: true,
        reason: "not_required_for_simak_ui",
      };
    } else if (forceReweight && ["sbmptn", "snbt"].includes(normalizedJenis)) {
      const autoWeightResult = await saveAdminStylePointsByTryout(conn, idTryout);
      weightSummary = {
        ...weightSummary,
        ...autoWeightResult,
        reason: "admin_simpan_semua_point_formula",
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
      await conn.query(`DROP TEMPORARY TABLE IF EXISTS tmp_latest_jawaban_umugm_source`);
      await conn.query(
        `
        CREATE TEMPORARY TABLE tmp_latest_jawaban_umugm_source AS
        SELECT *
        FROM tmp_latest_jawaban
        `
      );
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
              COALESCE(MAX(NULLIF(up.peminatan_user, '')), 'Saintek') AS peminatan,
              (
                SUM(
                  CASE
                    WHEN (jut.id_mapel = 51 OR UPPER(TRIM(mp.nama)) = 'TPA') THEN
                      CASE
                        WHEN jut.jawaban = st.kunci THEN 1
                        ELSE 0
                      END
                    ELSE
                      CASE
                        WHEN jut.jawaban = st.kunci THEN 4
                        WHEN COALESCE(jut.jawaban, '') = '' THEN 0
                        ELSE -1
                      END
                  END
                ) / 360
              ) * 1000 AS total
            FROM tmp_latest_jawaban jut
            JOIN (
              SELECT
                id_user,
                COALESCE(MAX(NULLIF(peminatan, '')), 'Saintek') AS peminatan_user
              FROM tmp_latest_jawaban_umugm_source
              WHERE id_tryout = ?
              GROUP BY id_user
            ) up
              ON up.id_user = jut.id_user
             AND LOWER(COALESCE(jut.peminatan, '')) = LOWER(up.peminatan_user)
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
        [idTryout, idTryout]
      );
    } else if (isSimakUi) {
      await conn.query(
        `
        INSERT INTO rank_tryout_2025
        (id_user, username, peminatan, total, instansi, provinsi, \`rank\`, id_tryout, year)
        SELECT
          r.id_user,
          r.username,
          r.peminatan,
          GREATEST(0, LEAST(1000, (r.raw_total / 420) * 1000)) AS total,
          ud.instansi,
          ud.provinsi,
          ROW_NUMBER() OVER (ORDER BY GREATEST(0, LEAST(1000, (r.raw_total / 420) * 1000)) DESC) AS rnk,
          ?,
          2026
        FROM (
          SELECT
            v.id_user,
            u.username,
            'ipc' AS peminatan,
            SUM((COALESCE(v.benar, 0) * 4) - COALESCE(v.salah, 0)) AS raw_total
          FROM jawaban_user_tryout_v2 v
          JOIN (
            SELECT MAX(id) AS max_id
            FROM jawaban_user_tryout_v2
            WHERE id_tryout = ?
            GROUP BY id_user, id_tryout, id_mapel
          ) lv ON lv.max_id = v.id
          JOIN users u ON u.id = v.id_user
          WHERE v.id_tryout = ?
          GROUP BY v.id_user, u.username
        ) r
        LEFT JOIN userdata ud ON ud.id_user = r.id_user;
        `,
        [idTryout, idTryout, idTryout]
      );
    } else if (isUmUns) {
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
              COALESCE(MAX(NULLIF(jut.peminatan, '')), 'ipc') AS peminatan,
              SUM(
                CASE
                  WHEN jut.id_mapel = 51 THEN
                    CASE
                      WHEN jut.jawaban = st.kunci THEN 4
                      WHEN COALESCE(jut.jawaban, '') = '' THEN 0
                      ELSE -1
                    END
                  WHEN jut.id_mapel IN (53, 54, 55) THEN
                    CASE
                      WHEN jut.jawaban = st.kunci THEN st.point * 100
                      ELSE 0
                    END
                  ELSE 0
                END
              ) AS total
            FROM tmp_latest_jawaban jut
            JOIN soal_tryout st
              ON st.no_soal = jut.no_soal
             AND st.id_mapel = jut.id_mapel
             AND st.id_tryout = jut.id_tryout
            JOIN users u ON u.id = jut.id_user
            WHERE jut.id_mapel IN (51, 53, 54, 55)
            GROUP BY jut.id_user, u.username
          ) n
        ) r
        LEFT JOIN userdata ud ON ud.id_user = r.id_user;
        `,
        [idTryout]
      );
    } else if (normalizedJenis === "tka") {
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
              scored.id_user,
              scored.username,
              COALESCE(MAX(NULLIF(scored.peminatan, '')), 'ipc') AS peminatan,
              /* Simpan agregat ranking pada skala IRT yang sama dengan
                 pengumuman: rata-rata skor tiap mata uji (200-800). */
              GREATEST(200, LEAST(800, AVG(200 + (scored.weighted_percent * 6)))) AS total
            FROM (
              SELECT
                jut.id_user,
                u.username,
                COALESCE(MAX(NULLIF(jut.peminatan, '')), 'ipc') AS peminatan,
                100 * COALESCE(
                  SUM(
                    CASE
                      WHEN jut.jawaban = st.kunci OR jut.status = 'benar'
                        THEN COALESCE(st.point, 0)
                      ELSE 0
                    END
                  ) / NULLIF(MAX(mt.total_point), 0),
                  SUM(CASE WHEN jut.jawaban = st.kunci OR jut.status = 'benar' THEN 1 ELSE 0 END)
                    / NULLIF(MAX(mt.total_soal), 0),
                  0
                ) AS weighted_percent
              FROM tmp_latest_jawaban jut
              JOIN soal_tryout st
                ON st.no_soal = jut.no_soal
               AND st.id_mapel = jut.id_mapel
               AND st.id_tryout = jut.id_tryout
              JOIN (
                SELECT
                  id_mapel,
                  SUM(COALESCE(point, 0)) AS total_point,
                  COUNT(DISTINCT no_soal) AS total_soal
                FROM soal_tryout
                WHERE id_tryout = ?
                GROUP BY id_mapel
              ) mt ON mt.id_mapel = jut.id_mapel
              JOIN users u ON u.id = jut.id_user
              GROUP BY jut.id_user, u.username, jut.id_mapel
            ) scored
            GROUP BY scored.id_user, scored.username
            HAVING COUNT(DISTINCT scored.id_mapel) >= 5
          ) n
        ) r
        LEFT JOIN userdata ud ON ud.id_user = r.id_user;
        `,
        [idTryout, idTryout]
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
              LEAST(
                CASE WHEN ? = 'tka' THEN 500 ELSE 999999999 END,
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
                )
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
        [idTryout, normalizedJenis, normalizedJenis, normalizedJenis, normalizedJenis]
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

    // Proses manual admin juga dianggap sebagai satu proses hasil untuk
    // membuka hasil sementara Premium, tanpa mengubah flow admin yang ada.
    if (["sbmptn", "snbt"].includes(normalizedJenis)) {
      await ensureNightlySchedulerTable(conn);
      const [scheduleRows] = await conn.query(
        `
        SELECT
          DATE_FORMAT(DATE(start_time), '%Y-%m-%d') AS first_process_date,
          DATE_FORMAT(DATE_ADD(DATE(end_time), INTERVAL 1 DAY), '%Y-%m-%d') AS final_process_date
        FROM tryout
        WHERE id = ?
        LIMIT 1
        `,
        [idTryout]
      );
      const schedule = scheduleRows[0] || {};
      const [successfulProcessRows] = await conn.query(
        `SELECT DATE_FORMAT(process_date, '%Y-%m-%d') AS process_date
         FROM tryout_nightly_process_log
         WHERE id_tryout = ? AND status = 'success'
         ORDER BY process_date ASC
         LIMIT 1`,
        [idTryout]
      );
      const processDate = schedulerProcessDate
        || (successfulProcessRows.length > 0 ? getJakartaDateParts().date : null)
        || schedule.first_process_date
        || getJakartaDateParts().date;
      if (!schedule.final_process_date || processDate <= schedule.final_process_date) {
        await conn.query(
          `
          INSERT INTO tryout_nightly_process_log
            (id_tryout, process_date, status, started_at, finished_at, error_message)
          VALUES (?, ?, 'success', NOW(), NOW(), NULL)
          ON DUPLICATE KEY UPDATE status = 'success', finished_at = NOW(), error_message = NULL
          `,
          [idTryout, processDate]
        );
      }
    }

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
      tka_scoring: normalizedJenis === "tka" ? {
        skala_nilai: "200-800",
        metode_nilai: "point-weighted-200-800",
        kategori: ["Kurang", "Memadai", "Baik"],
        kategori_estimasi: true,
        predikat_istimewa_minimum_setiap_mata_uji: 725,
      } : null,
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

app.post("/nightly-result-status", async (req, res) => {
  const { idTryout } = req.body;
  if (!idTryout) {
    return res.status(400).json({ success: false, message: "idTryout wajib diisi" });
  }

  const conn = await pool.getConnection();
  try {
    await ensureNightlySchedulerTable(conn);
    const [tryoutRows] = await conn.query(
      `
      SELECT
        id,
        jenis,
        tipe_tryout,
        start_time,
        end_time,
        DATE_FORMAT(DATE(start_time), '%Y-%m-%d') AS first_process_date,
        DATE_FORMAT(DATE_ADD(DATE(end_time), INTERVAL 1 DAY), '%Y-%m-%d') AS final_process_date
      FROM tryout
      WHERE id = ?
      LIMIT 1
      `,
      [idTryout]
    );

    if (!tryoutRows.length) {
      return res.status(404).json({ success: false, code: "TRYOUT_NOT_FOUND", message: "Tryout tidak ditemukan" });
    }

    const tryout = tryoutRows[0];
    const supported = [tryout.jenis, tryout.tipe_tryout]
      .map(normalizeJenis)
      .some((jenis) => jenis === "sbmptn" || jenis === "snbt");

    if (!supported) {
      return res.json({
        success: true,
        supported: false,
        ready: false,
        message: "Status proses malam hanya berlaku untuk tryout SNBT",
      });
    }

    const [successRows] = await conn.query(
      `
      SELECT process_date, finished_at
      FROM tryout_nightly_process_log
      WHERE id_tryout = ? AND status = 'success'
      ORDER BY process_date DESC, finished_at DESC
      LIMIT 1
      `,
      [idTryout]
    );
    const latestSuccess = successRows[0] || null;

    return res.json({
      success: true,
      supported: true,
      ready: Boolean(latestSuccess),
      firstScheduledAt: tryout.first_process_date ? `${tryout.first_process_date} 23:59:00` : null,
      finalScheduledAt: tryout.final_process_date ? `${tryout.final_process_date} 23:59:00` : null,
      lastProcessedAt: latestSuccess?.finished_at || null,
      lastProcessDate: latestSuccess?.process_date || null,
      timeZone: NIGHTLY_SCHEDULER_TIME_ZONE,
      message: latestSuccess
        ? "Hasil sementara sudah tersedia"
        : "Penilaian IRT masih diproses dan akan tersedia setelah proses malam pertama berhasil",
    });
  } catch (error) {
    console.error("[nightly-result-status] failed", { idTryout, message: error.message });
    return res.status(500).json({ success: false, error: error.message });
  } finally {
    conn.release();
  }
});

app.post("/process-tryout-user/status", async (req, res) => {
  const { idTryout, idUser } = req.body;
  if (!idTryout || !idUser) {
    return res.status(400).json({
      success: false,
      message: "idTryout dan idUser wajib diisi",
    });
  }

  try {
    const [pointRows, rankRows, answerRows] = await Promise.all([
      pool.query(
        `
        SELECT
          COUNT(*) AS total_soal,
          SUM(CASE WHEN point IS NOT NULL THEN 1 ELSE 0 END) AS soal_berbobot
        FROM soal_tryout
        WHERE id_tryout = ?
        `,
        [idTryout]
      ).then(([rows]) => rows),
      pool.query(
        `SELECT total, \`rank\` FROM rank_tryout_2025 WHERE id_tryout = ? AND id_user = ? LIMIT 1`,
        [idTryout, idUser]
      ).then(([rows]) => rows),
      pool.query(
        `
        SELECT COUNT(*) AS total_mapel, COALESCE(SUM(v.benar), 0) AS total_benar
        FROM jawaban_user_tryout_v2 v
        JOIN (
          SELECT MAX(id) AS max_id
          FROM jawaban_user_tryout_v2
          WHERE id_tryout = ? AND id_user = ?
          GROUP BY id_user, id_tryout, id_mapel
        ) x ON x.max_id = v.id
        `,
        [idTryout, idUser]
      ).then(([rows]) => rows),
    ]);

    const pointStatus = pointRows[0] || {};
    const totalSoal = Number(pointStatus.total_soal || 0);
    const soalBerbobot = Number(pointStatus.soal_berbobot || 0);
    const pointsReady = totalSoal > 0 && soalBerbobot === totalSoal;
    const totalBenar = Number(answerRows[0]?.total_benar || 0);
    const hasAnswers = Number(answerRows[0]?.total_mapel || 0) > 0;
    const rankTotal = rankRows.length > 0 && rankRows[0].total !== null
      ? Number(rankRows[0].total)
      : null;
    const staleZeroRank = pointsReady && totalBenar > 0 && rankTotal === 0;
    const rankReady = rankTotal !== null && !staleZeroRank;

    return res.json({
      success: true,
      data: {
        idTryout: Number(idTryout),
        idUser: Number(idUser),
        pointsReady,
        rankReady,
        hasAnswers,
        needsProcessing: hasAnswers && (!pointsReady || !rankReady),
        totalSoal,
        soalBerbobot,
        totalBenar,
        rankTotal,
        staleZeroRank,
      },
    });
  } catch (error) {
    console.error("[process-tryout-user/status] failed", error);
    return res.status(500).json({ success: false, error: error.message });
  }
});

// 🚀 API untuk proses pengumuman per-user (versi ringan untuk halaman user)
app.post("/process-tryout-user", async (req, res) => {
  const { idTryout, idUser, jenis, tipe_tryout } = req.body;
  const requestMeta = {
    idTryout,
    idUser,
    jenis: normalizeJenis(jenis || tipe_tryout || ""),
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
    const normalizedJenis = await resolveJenisTryout(conn, idTryout, jenis || tipe_tryout);
    const isKedinasan = isKedinasanJenis(normalizedJenis);
    const isUmUgm = isUmUgmJenis(normalizedJenis);
    const isSimakUi = isSimakUiJenis(normalizedJenis);
    const isUmUns = isUmUnsJenis(normalizedJenis);

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
      : isSimakUi
      ? await conn.query(
          `
          SELECT
            v.id_user,
            COALESCE(SUM((COALESCE(v.benar, 0) * 4) - COALESCE(v.salah, 0)), 0) AS raw_total,
            'ipc' AS peminatan
          FROM jawaban_user_tryout_v2 v
          JOIN (
            SELECT MAX(id) AS max_id
            FROM jawaban_user_tryout_v2
            WHERE id_tryout = ? AND id_user = ?
            GROUP BY id_user, id_tryout, id_mapel
          ) x ON x.max_id = v.id
          WHERE v.id_tryout = ? AND v.id_user = ?
          GROUP BY v.id_user
          `,
          [idTryout, idUser, idTryout, idUser]
        )
      : isUmUns
      ? await conn.query(
          `
          SELECT
            ju.id_user,
            COALESCE(SUM(
              CASE
                WHEN ju.id_mapel = 51 THEN
                  CASE
                    WHEN ju.jawaban = st.kunci THEN 4
                    WHEN COALESCE(ju.jawaban, '') = '' THEN 0
                    ELSE -1
                  END
                WHEN ju.id_mapel IN (53, 54, 55) THEN
                  CASE
                    WHEN ju.status = 'benar' THEN st.point * 100
                    ELSE 0
                  END
                ELSE 0
              END
            ), 0) AS raw_total,
            COALESCE(MAX(NULLIF(ju.peminatan, '')), 'ipc') AS peminatan
          FROM (
            SELECT j1.*
            FROM jawaban_user_tryout_pembahasan j1
            JOIN (
              SELECT MAX(id) AS max_id
              FROM jawaban_user_tryout_pembahasan
              WHERE id_tryout = ? AND id_user = ?
              GROUP BY id_user, id_tryout, id_mapel, no_soal
            ) lx ON lx.max_id = j1.id
          ) ju
          JOIN soal_tryout st
            ON st.no_soal = ju.no_soal
           AND st.id_mapel = ju.id_mapel
           AND st.id_tryout = ju.id_tryout
          WHERE ju.id_tryout = ? AND ju.id_user = ?
            AND ju.id_mapel IN (51, 53, 54, 55)
          GROUP BY ju.id_user
          `,
          [idTryout, idUser, idTryout, idUser]
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
                        WHEN ju.jawaban = st.kunci THEN 1
                        ELSE 0
                      END
                    ELSE
                      CASE
                        WHEN ju.jawaban = st.kunci THEN 4
                        WHEN COALESCE(ju.jawaban, '') = '' THEN 0
                        ELSE -1
                      END
                  END
                WHEN ? = 'simak ui' THEN
                  CASE
                    WHEN ju.jawaban = st.kunci THEN 4
                    WHEN COALESCE(ju.jawaban, '') = '' THEN 0
                    ELSE -1
                  END
                WHEN ju.status = 'benar' THEN
                  CASE
                    WHEN ? = 'tka' THEN 5
                    ELSE st.point * 100
                  END
                ELSE 0
              END
            ), 0) AS raw_total,
            COALESCE(MAX(NULLIF(ju.peminatan, '')), ${isUmUns ? "'ipc'" : "'Saintek'"}) AS peminatan
          FROM (
            SELECT j1.*
            FROM jawaban_user_tryout_pembahasan j1
            JOIN (
              SELECT MAX(id) AS max_id
              FROM jawaban_user_tryout_pembahasan
              WHERE id_tryout = ? AND id_user = ?
              GROUP BY id_user, id_tryout, id_mapel, no_soal
            ) lx ON lx.max_id = j1.id
          ) ju
          JOIN soal_tryout st
            ON st.no_soal = ju.no_soal
           AND st.id_mapel = ju.id_mapel
           AND st.id_tryout = ju.id_tryout
          JOIN mata_pelajaran mp ON mp.id = ju.id_mapel
          WHERE ju.id_tryout = ? AND ju.id_user = ?
            AND (
              ? <> 'simak ui'
              OR LOWER(COALESCE(ju.peminatan, '')) = 'ipc'
            )
          GROUP BY ju.id_user
          `,
          [normalizedJenis, normalizedJenis, normalizedJenis, idTryout, idUser, idTryout, idUser, normalizedJenis]
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

    let tkaSubjects = [];
    if (normalizedJenis === "tka") {
      const [tkaSubjectRows] = await conn.query(
        `
        SELECT
          st.id_mapel,
          mp.nama,
          COUNT(DISTINCT st.no_soal) AS jumlah_soal,
          SUM(CASE WHEN ju.jawaban = st.kunci OR ju.status = 'benar' THEN 1 ELSE 0 END) AS benar,
          SUM(
            CASE
              WHEN ju.jawaban = st.kunci OR ju.status = 'benar' THEN COALESCE(st.point, 0)
              ELSE 0
            END
          ) AS earned_point,
          SUM(COALESCE(st.point, 0)) AS total_point,
          (
            SUM(CASE WHEN ju.jawaban = st.kunci OR ju.status = 'benar' THEN 1 ELSE 0 END)
            / NULLIF(COUNT(DISTINCT st.no_soal), 0)
          ) * 100 AS legacy_percent
        FROM soal_tryout st
        JOIN mata_pelajaran mp ON mp.id = st.id_mapel
        LEFT JOIN (
          SELECT j1.*
          FROM jawaban_user_tryout_pembahasan j1
          JOIN (
            SELECT MAX(id) AS max_id
            FROM jawaban_user_tryout_pembahasan
            WHERE id_tryout = ? AND id_user = ?
            GROUP BY id_user, id_tryout, id_mapel, no_soal
          ) latest ON latest.max_id = j1.id
        ) ju
          ON ju.id_tryout = st.id_tryout
         AND ju.id_mapel = st.id_mapel
         AND ju.no_soal = st.no_soal
        WHERE st.id_tryout = ?
          AND st.id_mapel IN (
            SELECT DISTINCT id_mapel
            FROM jawaban_user_tryout_v2
            WHERE id_tryout = ? AND id_user = ?
          )
        GROUP BY st.id_mapel, mp.nama
        ORDER BY st.id_mapel ASC
        `,
        [idTryout, idUser, idTryout, idTryout, idUser]
      );
      tkaSubjects = tkaSubjectRows.map(buildTkaSubjectResult);
    }

    const rawTotal = Number(scoreRows[0].raw_total || 0);
    const userPeminatan = scoreRows[0].peminatan || ((normalizedJenis === "tka" || isKedinasan || isSimakUi || isUmUns) ? "ipc" : "Saintek");
    let finalTotal = (isUmUgm || isSimakUi)
      ? (rawTotal / (isSimakUi ? 420 : 360)) * 1000
      : (normalizedJenis === "tka" || isKedinasan
        ? rawTotal
        : rawTotal / 7);
    if (isSimakUi) {
      finalTotal = Math.max(0, Math.min(1000, finalTotal));
    } else if (normalizedJenis === "tka") {
      const subjectScores = tkaSubjects.map((subject) => Number(subject.nilai)).filter(Number.isFinite);
      const averageScore = subjectScores.length
        ? subjectScores.reduce((total, score) => total + score, 0) / subjectScores.length
        : 200;
      finalTotal = Math.max(200, Math.min(800, Number(averageScore.toFixed(2))));
    }

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
        nilai_tka: normalizedJenis === "tka"
          ? toTkaScaledScoreFromTotal(finalTotal, 5)
          : null,
        skala_nilai: normalizedJenis === "tka" ? "200-800" : null,
        metode_nilai: normalizedJenis === "tka"
          ? (tkaSubjects.length > 0 && tkaSubjects.every((subject) => subject.metode_nilai === "point-weighted-200-800")
            ? "point-weighted-200-800"
            : "fallback-linear")
          : null,
        kategori_per_mata_uji: normalizedJenis === "tka" ? tkaSubjects : [],
        kategori_estimasi: normalizedJenis === "tka",
        predikat_istimewa: normalizedJenis === "tka"
          ? hasTkaIstimewaPredicate(tkaSubjects, 5)
          : false,
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
          t.tipe_tryout,
          p.province_name,
          u.image
      FROM rank_tryout_2025 r
      JOIN users u ON u.id = r.id_user
      JOIN tryout t ON t.id = r.id_tryout
      LEFT JOIN province p ON p.id = r.provinsi
      WHERE r.id_tryout = ?
      ORDER BY r.total DESC
    `,
      [idTryout]
    );

    const data = rows.map((row) => {
      if (normalizeJenis(row.tipe_tryout) !== "tka") return row;
      return {
        ...row,
        nilai_tka: toTkaScaledScoreFromTotal(row.total, 5),
        skala_nilai: "200-800",
        metode_nilai: "aggregate-compatible-200-800",
      };
    });

    res.json({ success: true, data });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// jalankan server
const server = app.listen(2234, () => {
  console.log("Server pengumuman running on http://localhost:2234");
});

const nightlySchedulerTimer = setInterval(
  runNightlyTryoutScheduler,
  NIGHTLY_SCHEDULER_INTERVAL_MS
);
nightlySchedulerTimer.unref();

const initialNightlySchedulerTimer = setTimeout(runNightlyTryoutScheduler, 5000);
initialNightlySchedulerTimer.unref();

server.timeout = 600000; // untuk request
server.keepAliveTimeout = 620000; // jaga supaya koneksi gak putus duluan
