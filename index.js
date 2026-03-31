import express from "express";
import mysql from "mysql2/promise";
import Redis from "ioredis";

const app = express();
app.use(express.json());
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

const AUTO_WEIGHT_MIN_TOTAL = 9;
const AUTO_WEIGHT_MAX_TOTAL = 10;
const AUTO_WEIGHT_TARGET_TOTAL = 9.5;

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
  const targetTotal = Math.min(
    AUTO_WEIGHT_MAX_TOTAL,
    Math.max(AUTO_WEIGHT_MIN_TOTAL, AUTO_WEIGHT_TARGET_TOTAL)
  );

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
      ) * ?
      ELSE (? / mt.soal_count)
    END
    WHERE st.id_tryout = ?
  `,
    [targetTotal, targetTotal, idTryout]
  );

  return { updatedRows: updateResult.affectedRows || 0, mapelCount: mapels.length };
}

app.post("/simpan-jawaban-user/:id_tryout", async (req, res) => {
  try {
    const { id_tryout } = req.params;

    // 1. ambil jawaban_user_permapel
    const [rows] = await pool.query(
      "SELECT id_user,peminatan, jawaban_user_permapel FROM jawaban_user_tryout_v2 WHERE id_tryout = ?",
      [id_tryout]
    );

    if (rows.length === 0) {
      return res.status(404).json({ success: false, message: "Data tidak ditemukan" });
    }

    // 2. Parse semua JSON jadi array jawaban
    
    let allJawaban = [];
    rows.forEach(r => {
      try {
        const parsed = JSON.parse(r.jawaban_user_permapel);
        if (Array.isArray(parsed)) {
          // tambahkan id_user dari tabel luar ke setiap jawaban
          parsed.forEach(p => {
            p.id_user = r.id_user;
            p.peminatan = r.peminatan;
          });
          allJawaban.push(...parsed);
        }
      } catch (e) {
        console.error("JSON parse error:", e);
      }
    });

    if (allJawaban.length === 0) {
      return res.status(400).json({ success: false, message: "Tidak ada jawaban valid" });
    }

    await pool.query(
      `DELETE FROM jawaban_user_tryout WHERE id_tryout = ?`,
      [id_tryout]
    );
    // 3. mapping untuk bulk insert
    const values = allJawaban.map(j => [
      j.id_user,
      j.id_tryout,
      j.id_mapel,
      j.no_soal,
      j.status?.replace(/"/g, ""),
      j.jawaban?.replace(/"/g, ""),
      j.peminatan?.replace(/"/g, "")
    ]);

    const sql = `
      INSERT INTO jawaban_user_tryout 
      (id_user, id_tryout, id_mapel, no_soal, status, jawaban, peminatan)
      VALUES ?
    `;

    // 4. bulk insert
    const [result] = await pool.query(sql, [values]);

    res.json({
      success: true,
      inserted: result.affectedRows
    });
  } catch (err) {
    console.error("Bulk insert error:", err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// 🚀 API untuk generate ranking & simpan ke rank_tryout_2025
app.post("/process-tryout", async (req, res) => {
  const { idTryout,jenis } = req.body;
  const conn = await pool.getConnection();

  try {
    await conn.beginTransaction();

    await buildLatestJawabanTempTable(conn, idTryout);

    // 0. Auto set bobot soal berbasis performa user (benar/salah).
    // Bobot per mapel dinormalisasi pada total 9-10 (set default 9.5).
    const weightSummary = await autoSetBobotSoalByTryout(conn, idTryout);

    // 1. Hapus ranking lama biar tidak dobel
    await conn.query(
      `DELETE FROM rank_tryout_2025 WHERE id_tryout = ?`,
      [idTryout]
    );

    // 2. Hitung nilai + ranking (pakai CTE + ROW_NUMBER)
await conn.query(
  `
  INSERT INTO rank_tryout_2025
(id_user, username, peminatan, total, instansi, provinsi,  \`rank\`, id_tryout, year)
SELECT
  r.id_user,
  r.username,
  r.peminatan,
  r.total,
  u.instansi,
  u.provinsi,
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
      jut.peminatan,
      SUM(
        CASE 
          WHEN jut.status = 'benar' THEN 
            CASE 
              WHEN ? = 'tka' THEN 5 
              ELSE st.point * 100 
            END
          ELSE 0
        END
      ) ${jenis === 'tka' ? '' : '/ 7'} AS total
    FROM tmp_latest_jawaban jut
    JOIN soal_tryout st 
      ON st.no_soal = jut.no_soal
     AND st.id_mapel = jut.id_mapel
     AND st.id_tryout = jut.id_tryout
    JOIN users u ON u.id = jut.id_user
    GROUP BY jut.id_user, u.username
  ) n
) r
LEFT JOIN userdata u ON u.id_user = r.id_user;

  `,
  [idTryout, jenis]
);


    await conn.query(
      `delete
      FROM jawaban_user_tryout_pembahasan 
      WHERE id_tryout = ?
    `,
      [idTryout]
    );
    
    // 3. Copy jawaban user ke tabel pembahasan
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
     await conn.query(
      `delete
      FROM jawaban_user_tryout_pembahasan_v2 
      WHERE id_tryout = ?
    `,
      [idTryout]
    );
    await conn.query(
      `
      INSERT INTO jawaban_user_tryout_pembahasan_v2 
          (id,id_user,id_tryout,id_mapel,jawaban_user_permapel,peminatan,kosong,salah,benar)
      SELECT id,id_user,id_tryout,id_mapel,jawaban_user_permapel,peminatan,kosong,salah,benar
      FROM jawaban_user_tryout_v2 
      WHERE id_tryout = ?
    `,
      [idTryout]
    );

    await conn.commit();
    await redis.flushall();
    res.json({
      success: true,
      message: `Ranking & pembahasan berhasil diproses untuk tryout ${idTryout}`,
      auto_weighting: {
        mapel_processed: weightSummary.mapelCount,
        soal_updated: weightSummary.updatedRows,
        total_range: [AUTO_WEIGHT_MIN_TOTAL, AUTO_WEIGHT_MAX_TOTAL],
        target_total: AUTO_WEIGHT_TARGET_TOTAL,
      },
    });
  } catch (err) {
    await conn.rollback();
    console.error(err);
    res.status(500).json({ success: false, error: err.message });
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
