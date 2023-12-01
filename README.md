# concurrency-protocol
## Description
Ini merupakan proyek untuk memenuhi Tugas Besar Mata Kuliah IF3140 Manajemen Basis Data. Pada proyek ini diimplementasikan beberapa conccurency control protocol untuk sistem manajemen basis data, yaitu Two Phase Locking, OCC, dan MVCC. Proyek ini diimplementasikan menggunakan Python 3.

## Requirements
Python 3

## How to run
1. Lakukan clone terhadap repository
2. Pindah ke direktori proyek, kemudian pindah ke direktori /src
3. Jalankan perintah berikut pada terminal
```bash
python main.py -s <strategy> <schedule>
```
  Dengan strategy antara "2pl", "mvcc", dan "occ". Kemudian untuk schedule ditulis dalam format berikut
  ```bash
  <tipe operasi> <id transaksi> opsional : (<id data yang diakses>)
  ```
  Contoh
  ```bash
  R1(X); R2(X); R3(Z); R2(Y); W1(X); W2(Y); W3(Z); W2(X); C1; C2; C3;
  ```
