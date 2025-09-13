import express from "express";
import mysql from "mysql2/promise";

const app = express();
app.use(express.json());

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

app.post("/simpan-jawaban-user/:id_tryout", async (req, res) => {
  try {
    const { id_tryout } = req.params;

    // 1. ambil jawaban_user_permapel
    const [rows] = await pool.query(
      "SELECT jawaban_user_permapel FROM jawaban_user_tryout_v2 WHERE id_tryout = ?",
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
          allJawaban.push(...parsed);
        }
      } catch (e) {
        console.error("JSON parse error:", e);
      }
    });

    if (allJawaban.length === 0) {
      return res.status(400).json({ success: false, message: "Tidak ada jawaban valid" });
    }

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
app.post("/process-tryout/:idTryout", async (req, res) => {
  const { idTryout } = req.params;
  const conn = await pool.getConnection();

  try {
    await conn.beginTransaction();

    // 1. Hapus ranking lama biar tidak dobel
    await conn.query(
      `DELETE FROM rank_tryout_2025 WHERE id_tryout = ?`,
      [idTryout]
    );

    // 2. Hitung nilai + ranking (pakai CTE + ROW_NUMBER)
 await conn.query(
  `
  INSERT INTO rank_tryout_2025
    (id_user, username, peminatan, total, instansi, provinsi, \`rank\`, id_tryout, year)
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
        SUM(CASE WHEN jut.status = 'benar' THEN st.point*100 ELSE 0 END) / 7 AS total
      FROM jawaban_user_tryout jut
      JOIN soal_tryout st 
        ON st.no_soal = jut.no_soal 
       AND st.id_mapel = jut.id_mapel 
       AND st.id_tryout = jut.id_tryout
      JOIN users u ON u.id = jut.id_user
      WHERE jut.id_tryout = ?
      GROUP BY jut.id_user, u.username, jut.peminatan
    ) n
  ) r
  LEFT JOIN userdata u ON u.id_user = r.id_user;
  `,
  [idTryout, idTryout]
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
    res.json({ success: true, message: `Ranking & pembahasan berhasil diproses untuk tryout ${idTryout}` });
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
app.listen(2234, () => {
  console.log("Server pengumuman running on http://localhost:2234");
});
