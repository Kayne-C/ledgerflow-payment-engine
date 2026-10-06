# Ölçümler

Bu sayfa iki soruya cevap verir: **(1) sistem yük altında ne kadar hızlı?** ve **(2) bir şeyler bozulduğunda doğru kalıyor
mu?** İkincisi bir ödeme motoru için daha önemlidir. Bu yüzden önce kaos tatbikatı, sonra performans gelir.

## Ortam

| | |
|---|---|
| Makine | Tek VM, **4 vCPU**, 16 GB RAM, Linux 6.x |
| Topoloji | `docker compose`: API, **2 processor** (her biri 4 tüketici thread'i), risk (gRPC), SQL Server 2022, Kafka 4.1 (KRaft, 24 partition), Redis 7, Aspire Dashboard. Hepsi **aynı makinede** |
| Yük üreteci | k6 v1.3, **aynı makinede**, `constant-arrival-rate` (sabit varış hızı, coordinated omission'a karşı) |
| Override | Yük testi için API rate limit'i yükseltildi ve log seviyesi `Warning`'e çekildi. Ayarlar dışında kod aynıdır |

> **Önemli:** Veritabanı, broker, 4 servis ve yük üreteci aynı 4 çekirdeği paylaşıyor. Aşağıdaki sayılar bir kapasite
> iddiası değil, **tekrar üretilebilir bir taban çizgisidir**. Doyum noktasında makinenin load average değeri ~11 idi,
> yani darboğaz uygulama değil CPU paylaşımıydı (aşağıda nasıl izole edildiği anlatılıyor).

## 1. Kaos tatbikatı

```bash
docker compose up -d --build
ACCOUNTS=200 tests/chaos/chaos-drill.sh 30 100s
```

30 ödeme/sn sabit yük, 100 sn, 200 hesap arasında rastgele ödemeler. Yük sürerken şu adımlar uygulanır:

| t (ödemeler başladıktan sonra) | Olay |
|---|---|
| +20 sn | Processor replikalarından biri **SIGKILL** ile öldürülür (graceful shutdown yok, offset commit'i yok) |
| +36 sn | Risk servisi durdurulur (**20 sn** boyunca hiçbir risk kararı verilemez) |
| +56 sn | Risk servisi geri gelir |
| +67 sn | Öldürülen processor geri gelir (Kafka consumer group rebalance) |
| +77 sn | **Kafka yeniden başlatılır** |

Sonuç:

| Ölçüt | Sonuç |
|---|---|
| HTTP istekleri | 3.401 (200 hesap açma + 200 fonlama + 3.001 ödeme), **%0 hata** |
| Ödeme sonuçları | **3.001 Completed**, 0 Failed |
| Çift mutabakat (2'den farklı sayıda yevmiye satırı olan işlem) | **0** |
| DLQ'ya düşen komut | 31. Risk kesintisi sırasında 5 denemesini tüketen `AssessPaymentRisk` mesajları |
| Yük bittikten sonra süreçte kalan ödeme | **85 sn içinde 0**. DLQ'daki adımları sweeper (2 dk eşiği) yeniden yürüttü |
| Mutabakat raporu | `isClean: true`: 654 hesap, 24.900 yevmiye satırı, issue yok |
| Deneme bilançosu | müşteri toplamı 650.030.000,00 ₺, clearing toplamı −650.030.000,00 ₺, **net 0** (veritabanı önceki koşuların hesaplarını da içeriyor) |

**Gözlemler**

- Processor SIGKILL edildiğinde, onun işlediği ama offset'ini saklamadığı mesajlar diğer replikaya yeniden teslim
  edildi. Adımlar idempotent olduğu için hiçbiri iki kez etki etmedi.
- Risk kesintisinde hiçbir ödeme varsayılan olarak onaylanmadı ve hiçbir blokenin parası kaybolmadı. Retry'lar tükenince
  mesaj DLQ'ya düştü. Sweeper adımı yeniden yayımladı ve ödemeler tamamlandı.
- Kafka restart'ında outbox relay yayımlayamadığı satırları üstel geri çekilmeyle bekletti. Broker dönünce kuyruk eridi.

**İlk denemeden bir not.** Aynı tatbikat ilk olarak **50 hesapla** koşturuldu. 30 ödeme/sn ÷ 50 hesap, hesap başına
dakikada ~36 ödeme demektir ve bu, risk motorunun hız (velocity) kuralını (dakikada > 30) aşar. Sonuçta 2.196 ödeme
tamamlandı, **805 ödeme `Risk.Rejected` ile reddedildi ve blokeleri doğru şekilde çözüldü**. O koşuda da çift mutabakat 0
ve mutabakat temizdi (454 hesap, 18.498 yevmiye satırı). Bu bir hata değil, kuralın doğru çalıştığının kanıtıdır. Ancak
tatbikatın amacı kesintileri ölçmek olduğu için betik artık varsayılan olarak 200 hesap kullanıyor.

## 2. Uçtan uca saga performansı

```bash
tests/load/measure-e2e.sh 40 60s
```

Betik k6 ile sabit hızda ödeme gönderir, tüm ödemeler son durumlarına ulaşana kadar bekler, sonra gecikmeyi doğrudan read
model'den (`PaymentView.InitiatedAtUtc → FinishedAtUtc`) okur. Ölçülen yol: API commit → outbox relay → Kafka → reserve →
gRPC risk → settle (4 stream + 2 yevmiye satırı, tek transaction).

| Sabit yük | Uçtan uca verim | Saga p50 | Saga p95 | Saga p99 | API p95 (202 Accepted) | Mutabakat |
|---|---|---|---|---|---|---|
| **40 ödeme/sn** | 39,9 /sn | **120 ms** | **212 ms** | **278 ms** | 28,6 ms | temiz |
| 60 ödeme/sn | 59,9 /sn | 252 ms | 3.180 ms | 4.474 ms | 61 ms | temiz |
| 70 ödeme/sn | doyum: kuyruk büyüyor, makine CPU sınırında | — | — | — | — | — |

60/sn'de verim hâlâ girdiye eşit, ama kuyruk birikmeye başladığı için kuyruk gecikmesi (tail latency) saniyeler
mertebesine çıkıyor. Bu, CPU'nun tükenmeye başladığı noktadır.

## 3. SQL Server ayarı: ölçerek

İlk tam yük denemesinde API ~100 istek/sn'de doyuma ulaştı ve ortalama yanıt süresi ~6 sn'ye, p95 13–16 sn'ye çıktı.
Mutabakat temizdi, yani sorun doğrulukta değil performanstaydı. Adım adım:

**1) Bekleme istatistikleri** (`sys.dm_os_wait_stats`, bir yük koşusu boyunca):

| wait type | toplam bekleme |
|---|---|
| `PAGELATCH_EX` | 187,7 sn |
| `WRITELOG` | 140,4 sn |
| `PAGELATCH_SH` | 124,6 sn |
| `LCK_M_U` | 83,1 sn |

Tablo, `Events`, `Outbox` ve `LedgerEntries` identity PK'lerinin **son sayfasına** eşzamanlı insert çekişmesini
gösteriyor. Bu, artan anahtarlı tablolarda bilinen bir sorundur. Deadlock yoktu.

**2) Düzeltme:** ayrı bir migration (`SequentialKeyInsertTuning`):

```sql
ALTER INDEX PK_Events        ON Events        SET (OPTIMIZE_FOR_SEQUENTIAL_KEY = ON);
ALTER INDEX PK_Outbox        ON Outbox        SET (OPTIMIZE_FOR_SEQUENTIAL_KEY = ON);
ALTER INDEX PK_LedgerEntries ON LedgerEntries SET (OPTIMIZE_FOR_SEQUENTIAL_KEY = ON);
ALTER DATABASE CURRENT SET AUTO_UPDATE_STATISTICS_ASYNC ON;
ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE;
```

Aynı yükte `PAGELATCH_EX` beklemesi **187 sn → 13 sn**'ye düştü. `READ_COMMITTED_SNAPSHOT`, okuma tarafındaki read
model sorgularının yazarları bloklamasını önler.

**3) Darboğazın izolasyonu.** Processor'lar durdurulup API tek başına ölçüldü: **150 istek/sn'de p95 17,7 ms**. Yani API
ve event store yazma yolu tek başına hızlıydı. Asıl sınır, saga tüketicileri, risk servisi, Kafka ve SQL Server'ın aynı 4
çekirdeği paylaşmasıydı (load average ~11).

**4) CPU başına iş azaltma:**

- Süreç içi hesap önbelleği: sıcak hesaplar her adımda yeniden hydrate edilmez.
- Snapshot aralığı 25 olay.
- `AddDbContextPool`: her mesaj için yeni `DbContext` kurulmaz.
- Process başına 4 tüketici thread'i.
- Optimistic retry'larda üstel geri çekilme + full jitter, böylece çakışan yazarlar senkron tekrar denemez.

Sonuç: yukarıdaki tablo (40/sn'de saga p95 212 ms).

## Tekrar üretme

```bash
docker compose up -d --build
# Varsayılan limit (istemci başına 500 istek/sn, burst 1.000) bu hızlar için yeterli. Daha yüksek hızlarda
# RateLimiting__PermitsPerSecondPerClient / RateLimiting__BurstPerClient değerlerini yükseltin.
tests/load/measure-e2e.sh 40 60s
tests/load/measure-e2e.sh 60 60s
ACCOUNTS=200 tests/chaos/chaos-drill.sh 30 100s
```

## Sonraki ölçümler

- Veritabanını, Kafka'yı ve servisleri **ayrı makinelere** dağıtıp yatay ölçekleme eğrisini çıkarmak (processor sayısı ×
  partition sayısı).
- Oracle 23ai üzerinde aynı saga ölçümü (Free sürümü 2 CPU thread'i ile sınırlıdır, ayrı bir taban gerekir).
- Tek sıcak hesap (merchant çıkışı) senaryosu: partition başına tek yazıcının sınırı.
