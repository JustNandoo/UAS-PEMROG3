# ojk-fintech-airflow

Otomasi pemantauan risiko kredit fintech lending (data OJK) dengan Apache Airflow.
Proyek UAS Pemrograman 3, **Kelompok 3**, fokus: **trigger dan automation antar-DAG**.

Repo: https://github.com/JustNandoo/UAS-PEMROG3

> **Baru gabung?** Baca bagian [Progress](#progress) dulu untuk tahu apa yang sudah jadi dan apa yang masih perlu dikerjakan, lalu ikuti [Memulai](#memulai) untuk menjalankan proyek di laptop sendiri.

---

## Progress

Status per 12 November 2026. Presentasi di minggu ke 14-15.

### Ringkasan

Kode pipeline (3 DAG) **sudah jadi dan sudah berjalan sukses** dari awal sampai akhir (DAG 1 → DAG 2 → DAG 3). Yang belum: bukti monitoring dan error handling, laporan PDF, slide, dan refleksi individu.

### Checklist tahapan (sesuai modul UAS)

| Tahap modul | Output yang diminta | Status |
|---|---|---|
| 1. Inisiasi & desain | Dataset + diagram DAG | ✅ Dataset dan alur DAG sudah ada. ⬜ Diagram untuk laporan belum dibuat (bisa pakai Graph View Airflow) |
| 2. Implementasi ETL | Skrip ETL dalam file DAG `.py` | ✅ Selesai |
| 3. Trigger & schedule | Konfigurasi DAG & parameter trigger | ✅ Selesai (`@monthly`, `TriggerDagRunOperator`, `ExternalTaskSensor`, `BranchPythonOperator`, `conf`) |
| 4. Monitoring & logging | Screenshot monitoring & error log | ⬜ Belum. Sejauh ini semua run sukses, belum ada contoh run gagal |
| 5. Presentasi & laporan | Slide, laporan PDF, refleksi individu | ⬜ Belum |

### Yang masih perlu dikerjakan

**A. Monitoring & error handling (bobot nilai 20%)**
- [ ] Skenario gagal 1: batch pinjaman berisi data rusak, sehingga `validate_loan_batch_quality` di DAG 1 gagal. Ambil screenshot log error.
- [ ] Skenario 2: periode dengan TWP90 ≤ 4%, sehingga DAG 2 masuk cabang `skip_compliance_alert` dan DAG 3 tidak terpicu. Cabang ini belum pernah dijalankan.
- [ ] Skenario gagal 3: picu DAG 2 sendirian, sehingga sensor menunggu 180 detik lalu timeout.
- [ ] (Opsional) Tambah `on_failure_callback` atau SLA agar error tercatat lebih rapi.
- [ ] Screenshot Airflow UI: Graph View, Grid View, dan Log untuk run sukses maupun gagal.

**B. Perbaikan kode**
- [ ] `dags/dag_2_risk_portfolio_analytics.py`, fungsi `evaluate_risk_threshold`: hapus nilai cadangan `4.32` saat XCom kosong. Sekarang kegagalan jadi tertutup dan alert tetap menyala.
- [ ] `dags/dag_3_compliance_alert_automation.py`, fungsi `generate_regulatory_audit_log`: hapus nilai cadangan (`'4.32'`, `'3. Jawa Barat'`, dll.). Kalau file mart tidak ada, task seharusnya gagal, bukan menerbitkan laporan dengan angka karangan.
- [ ] Putuskan apakah TWP90 batch (2,1%) ikut dipakai dalam keputusan alert. Sekarang hanya TWP90 nasional yang dipakai.
- [ ] DAG 2 belum membaca `conf` yang dikirim DAG 1. Pakai, atau jelaskan di laporan.
- [ ] Dua dataset belum dipakai (`ojk_regional_exposure.csv`, `pinjaman_german_credit.csv`). Pakai, atau hapus.
- [ ] (Nilai tambah) Tambah mekanisme trigger berbasis **Dataset** (data-aware scheduling Airflow 2.4+) sebagai pembanding cara memicu DAG.

**C. Dokumen**
- [ ] Laporan PDF, ikuti template modul:
  1. Identitas proyek
  2. Ringkasan
  3. Desain DAG
  4. Implementasi & kode
  5. Hasil & monitoring
  6. Evaluasi & refleksi
- [ ] Slide presentasi: pembukaan, konsep & alur, demo, refleksi.
- [ ] Refleksi individu, 1 halaman per orang.
- [ ] Isi tabel [Pembagian kerja](#pembagian-kerja).
- [ ] Jelaskan di laporan kenapa jadwal `@monthly`: data OJK terbit bulanan, sedangkan modul mencontohkan jadwal harian/mingguan.

### Rubrik penilaian (pengingat)

| Aspek | Bobot |
|---|---|
| Desain dan arsitektur | 25% |
| Implementasi berjalan baik | 25% |
| Monitoring dan analisis hasil | 20% |
| Kualitas presentasi dan komunikasi | 20% |
| Refleksi individu & kolaborasi tim | 10% |

Kolaborasi tim ikut dinilai, jadi usahakan setiap anggota punya commit sendiri di repo ini.

---

## Alur pipeline

```
ojk_ingest_and_validate_pipeline   (@monthly)
  ingest ringkasan OJK -> validasi batch pinjaman
  -> TriggerDagRunOperator
        |
        v
ojk_risk_portfolio_analytics       (dipicu DAG 1)
  ExternalTaskSensor -> agregasi risiko -> BranchPythonOperator
  TWP90 > 4,0% ? -> TriggerDagRunOperator
        |
        v
ojk_compliance_alert_automation    (dipicu DAG 2, hanya jika TWP90 > 4,0%)
  log audit -> notifikasi (simulasi) -> arsip snapshot (simulasi)
```

| DAG | File | Jadwal | Mekanisme antar-DAG |
|---|---|---|---|
| `ojk_ingest_and_validate_pipeline` | `dags/dag_1_ojk_ingest_validate.py` | `@monthly` | `TriggerDagRunOperator` ke DAG 2 |
| `ojk_risk_portfolio_analytics` | `dags/dag_2_risk_portfolio_analytics.py` | tanpa jadwal | `ExternalTaskSensor` ke DAG 1, `BranchPythonOperator`, trigger bersyarat ke DAG 3 |
| `ojk_compliance_alert_automation` | `dags/dag_3_compliance_alert_automation.py` | tanpa jadwal | dipicu DAG 2 |

### Hasil run terakhir

| Metrik | Nilai |
|---|---|
| Periode | 2025-12 |
| TWP90 nasional | 4,32% (di atas ambang 4,0%, jadi alert menyala) |
| Jumlah pinjaman di batch | 1.000 |
| Total penyaluran batch | Rp 5.000.000.000 |
| Total kredit macet batch | Rp 80.000.000 (TWP90 batch 2,1%) |
| Provinsi kredit macet tertinggi | DKI Jakarta (Rp 20.500.000) |

## Struktur proyek

```
dags/               kode DAG (3 file)
data/raw/           data mentah
data/processed/     data yang dibaca DAG
data/output/        hasil DAG (dibuat otomatis, tidak masuk git)
scripts/setup.sh    membuat .venv dan memasang Airflow
scripts/run_airflow.sh   menjalankan Airflow lokal
scripts/macos_fix/  perbaikan crash Airflow di macOS
requirements.txt
```

Folder lokal yang tidak masuk git: `.venv/`, `.airflow/` (database dan log Airflow), `data/output/`.

## Memulai

Prasyarat:
- Git.
- Python 3.10-3.12 (diuji dengan 3.12). Airflow 2.10 belum mendukung Python 3.13 ke atas.
- Koneksi internet untuk memasang paket.
- Windows: jalankan lewat **WSL** (Ubuntu). Airflow tidak berjalan langsung di Windows.

```bash
git clone https://github.com/JustNandoo/UAS-PEMROG3.git
cd UAS-PEMROG3
./scripts/setup.sh
./scripts/run_airflow.sh
```

Buka http://localhost:8080 dengan user `admin` dan password `admin`. Untuk menggantinya, set `AIRFLOW_ADMIN_USER` dan `AIRFLOW_ADMIN_PASSWORD` sebelum menjalankan skrip. Hentikan Airflow dengan Ctrl+C.

## Menjalankan pipeline

Semua DAG awalnya nonaktif. Cara termudah: di UI, aktifkan ketiga DAG, lalu picu `ojk_ingest_and_validate_pipeline`.

Bisa juga lewat terminal lain, setelah Airflow menyala:

```bash
export AIRFLOW_HOME="$PWD/.airflow" AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags" PATH="$PWD/.venv/bin:$PATH"
airflow dags unpause ojk_compliance_alert_automation
airflow dags unpause ojk_risk_portfolio_analytics
airflow dags unpause ojk_ingest_and_validate_pipeline
airflow dags trigger ojk_ingest_and_validate_pipeline
```

Selalu mulai dari DAG 1. Sensor di DAG 2 mencari run DAG 1 dengan tanggal logis yang sama. Kalau DAG 2 dipicu sendirian, ia menunggu 180 detik lalu gagal.

Hasilnya ada di `data/output/`: `ojk_portfolio_risk_mart.csv` dan `compliance_alert_log.txt`.

## Dataset

| File | Isi | Dipakai DAG | Sumber |
|---|---|---|---|
| `data/processed/ojk_monthly_summary.csv` | TKB90, TWP90, ROA, ROE nasional per bulan (Des 2024-Des 2025) | DAG 1, DAG 2 | [Statistik LPBBTI Desember 2025, OJK](https://ojk.go.id/id/kanal/iknb/data-dan-statistik/fintech/Pages/Statistik-LPBBTI-Desember-2025.aspx) |
| `data/processed/fintech_loan_daily_batch.csv` | 1000 pinjaman per peminjam | DAG 1, DAG 2 | Tidak berasal dari publikasi OJK (OJK tidak merilis data per peminjam). Perlakukan sebagai data simulasi. |
| `data/processed/ojk_regional_exposure.csv` | Penyaluran kumulatif per provinsi (Des 2025) | tidak dipakai | Statistik LPBBTI OJK, Tabel 18 |
| `data/raw/pinjaman_german_credit.csv` | 1000 pemohon kredit di Jerman (1994) | tidak dipakai | [UCI Statlog German Credit](https://archive.ics.uci.edu/dataset/144/statloggermancreditdata) |

## Pembagian kerja

Isi nama dan bagian masing-masing (lihat daftar tugas di [Progress](#progress)).

| Nama | Bagian |
|---|---|
| | |

## Cara berkontribusi

1. Ambil perubahan terbaru sebelum mulai kerja:
   ```bash
   git checkout main && git pull
   ```
2. Buat branch sendiri, misalnya `git checkout -b monitoring-skenario-gagal`.
3. Kerjakan, lalu cek tidak ada error di DAG:
   ```bash
   airflow dags list-import-errors
   ```
4. Commit, push branch, lalu buka pull request ke `main` di GitHub:
   ```bash
   git add . && git commit -m "pesan singkat" && git push -u origin <nama-branch>
   ```
5. Centang checklist di bagian [Progress](#progress) kalau tugasnya sudah selesai.

Jangan commit `.venv/`, `.airflow/`, atau `data/output/`. Semuanya sudah diabaikan lewat `.gitignore`.

## Catatan macOS

Di macOS, paket `setproctitle` bawaan Airflow membuat proses anak crash dengan `SIGSEGV` ("Python quit unexpectedly") setelah `fork()`. `scripts/run_airflow.sh` mengatasinya dengan menaruh versi kosong di `scripts/macos_fix/` ke `PYTHONPATH`. Paket aslinya hanya mengubah nama proses, jadi pipeline tidak terpengaruh. Di Linux perbaikan ini tidak dipakai.
