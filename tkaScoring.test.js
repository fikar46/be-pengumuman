import test from "node:test";
import assert from "node:assert/strict";
import {
  buildTkaSubjectResult,
  calculateTkaAggregateScore,
  getEstimatedTkaCategory,
  hasTkaIstimewaPredicate,
  toTkaScaledScore,
  toTkaScaledScoreFromTotal,
  toTkaScaledScoreFromPoints,
} from "./tkaScoring.js";

test("mengubah proxy nilai lama ke skala TKA 200-800", () => {
  assert.equal(toTkaScaledScore(0), 200);
  assert.equal(toTkaScaledScore(50), 500);
  assert.equal(toTkaScaledScore(100), 800);
  assert.equal(toTkaScaledScoreFromTotal(250, 5), 500);
});

test("mengubah rasio point soal menjadi skala TKA 200-800", () => {
  assert.equal(toTkaScaledScoreFromPoints(0, 10), 200);
  assert.equal(toTkaScaledScoreFromPoints(7, 10), 620);
  assert.equal(toTkaScaledScoreFromPoints(8.75, 10), 725);
  assert.equal(toTkaScaledScoreFromPoints(10, 10), 800);
  assert.equal(toTkaScaledScoreFromPoints(0, 0, 70), 620);
});

test("mengirim kategori estimasi per mata uji", () => {
  const result = buildTkaSubjectResult({
    id_mapel: 70,
    nama: "Sosiologi",
    legacy_percent: 70,
    benar: 14,
    jumlah_soal: 20,
    earned_point: 7,
    total_point: 10,
  });
  assert.equal(result.nilai, 620);
  assert.equal(result.kategori, "Baik");
  assert.equal(result.kategori_estimasi, true);
  assert.equal(result.metode_nilai, "point-weighted-200-800");
  assert.equal(getEstimatedTkaCategory(499.99), "Kurang");
});

test("predikat Istimewa mensyaratkan semua lima mata uji minimal 725", () => {
  const complete = Array.from({ length: 5 }, () => ({ nilai: 725 }));
  assert.equal(hasTkaIstimewaPredicate(complete), true);
  assert.equal(hasTkaIstimewaPredicate([...complete.slice(0, 4), { nilai: 724.99 }]), false);
  assert.equal(hasTkaIstimewaPredicate(complete.slice(0, 4)), false);
});

test("agregat TKA selalu memperhitungkan minimal lima mata uji", () => {
  assert.equal(calculateTkaAggregateScore([{ nilai: 800 }]), 320);
  assert.equal(calculateTkaAggregateScore([
    { nilai: 500 },
    { nilai: 550 },
    { nilai: 600 },
    { nilai: 650 },
    { nilai: 700 },
  ]), 600);
});
