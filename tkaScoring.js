export const TKA_MIN_SCORE = 200;
export const TKA_MAX_SCORE = 800;
export const TKA_ISTIMEWA_SCORE = 725;

const clamp = (value, min, max) => Math.max(min, Math.min(max, value));

export function toTkaScaledScore(legacyPercent) {
  const numericValue = Number(legacyPercent);
  const safePercent = Number.isFinite(numericValue) ? clamp(numericValue, 0, 100) : 0;
  return Number((TKA_MIN_SCORE + (safePercent * 6)).toFixed(2));
}

export function toTkaScaledScoreFromTotal(legacyTotal, subjectCount = 5) {
  const safeSubjectCount = Math.max(1, Number(subjectCount) || 5);
  return toTkaScaledScore(Number(legacyTotal || 0) / safeSubjectCount);
}

export function toTkaScaledScoreFromPoints(earnedPoint, totalPoint, fallbackPercent = 0) {
  const earned = Number(earnedPoint);
  const possible = Number(totalPoint);
  if (Number.isFinite(earned) && Number.isFinite(possible) && possible > 0) {
    return Number((TKA_MIN_SCORE + (clamp(earned / possible, 0, 1) * 600)).toFixed(2));
  }
  return toTkaScaledScore(fallbackPercent);
}

export function getEstimatedTkaCategory(score) {
  const safeScore = clamp(Number(score) || TKA_MIN_SCORE, TKA_MIN_SCORE, TKA_MAX_SCORE);
  if (safeScore >= 620) return "Baik";
  if (safeScore >= 500) return "Memadai";
  return "Kurang";
}

export function buildTkaSubjectResult(row = {}) {
  const legacyPercent = Number(row.legacy_percent || 0);
  const earnedPoint = Number(row.earned_point || 0);
  const totalPoint = Number(row.total_point || 0);
  const score = toTkaScaledScoreFromPoints(earnedPoint, totalPoint, legacyPercent);
  return {
    id_mapel: Number(row.id_mapel),
    nama: row.nama || "",
    nilai: score,
    kategori: getEstimatedTkaCategory(score),
    kategori_estimasi: true,
    metode_nilai: totalPoint > 0 ? "point-weighted-200-800" : "fallback-linear",
    point_diperoleh: Number(earnedPoint.toFixed(6)),
    total_point: Number(totalPoint.toFixed(6)),
    benar: Number(row.benar || 0),
    jumlah_soal: Number(row.jumlah_soal || 0),
  };
}

export function hasTkaIstimewaPredicate(subjects = [], expectedSubjectCount = 5) {
  return Array.isArray(subjects)
    && subjects.length >= expectedSubjectCount
    && subjects.every((subject) => Number(subject.nilai) >= TKA_ISTIMEWA_SCORE);
}
