# ADR-0001: Event store'u ilişkisel veritabanı üzerinde kurmak

- **Durum:** Kabul edildi
- **Tarih:** 2026-09

## Bağlam

Defterin doğruluk kaynağı değişmez olaylardır. Bir ödemenin mutabakatı 4 stream'e (işlem, iki hesap, ödeme) aynı anda
yazmayı, read model'leri güncellemeyi ve sonraki adımın mesajını yayımlamayı gerektirir. Bunların hepsi ya birlikte
olmalı ya hiç olmamalıdır. Hedef kurumlarda SQL Server veya Oracle zaten işletilmektedir: yedekleme, HA/DR, denetim ve
DBA yetkinliği mevcuttur.

## Seçenekler

1. **EventStoreDB / KurrentDB**: olay deposu olarak olgun. Ama read model ve outbox için ikinci bir veritabanı gerekir,
   bu da aralarında dağıtık tutarlılık problemi demektir. Bankalarda yeni bir veri ürünü onaylatmak da ayrı bir maliyettir.
2. **Kafka'yı event store olarak kullanmak**: stream başına optimistic concurrency (beklenen versiyon) yoktur. Tek bir
   hesabın bakiyesini okumak için log'u taramak gerekir.
3. **SQL Server / Oracle üzerinde tablo tabanlı event store (seçilen)**: `(StreamId, StreamVersion)` unique index'i
   concurrency kontrolünü, tek transaction da olay + projeksiyon + outbox atomikliğini verir.

## Karar

Event store, read model'ler ve outbox aynı veritabanında, EF Core 10 ile tutulur. SQL Server ve Oracle için ayrı
migration setleri vardır. Hız için snapshot (her 25 olayda bir) ve süreç içi hesap önbelleği kullanılır. Kurcalamaya
karşı iz için stream başına SHA-256 hash zinciri tutulur.

## Sonuçlar

- (+) Dağıtık transaction olmadan tam atomiklik. Mevcut DB operasyonu (yedek, Always On / Data Guard) aynen kullanılır.
- (+) Okuma modelleri olayla aynı anda tutarlıdır (inline projeksiyon).
- (−) Yazma kapasitesi tek veritabanının kapasitesiyle sınırlıdır. Identity PK üzerinde son sayfa çekişmesi (last-page
  insert contention) ölçülerek giderildi (bkz. benchmarks).
- (−) Olay şeması evrimi (upcasting) bizim sorumluluğumuzdadır. Kapalı bir tip kaydı (`EventSerializer`) bilinmeyen tipi
  reddeder.
