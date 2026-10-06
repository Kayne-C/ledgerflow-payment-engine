# ADR-0005: Transactional outbox ve hesaba göre Kafka partition'lama

- **Durum:** Kabul edildi
- **Tarih:** 2026-09

## Bağlam

Bir saga adımının sonucu (olaylar) ile bir sonraki adımın mesajı birlikte kalıcı olmalıdır. Veritabanına yazıp Kafka'ya
gönderememek, ya da Kafka'ya gönderip commit edememek, takılan veya hayalet ödemeler üretir. Ayrıca aynı hesaba
eşzamanlı yazımlar optimistic concurrency çakışmasına, yani boşa harcanan retry'lara yol açar.

## Karar

- **Outbox:** Mesajlar olaylarla aynı transaction'da `Outbox` tablosuna yazılır. Processor'lardaki relay'ler bir id
  aralığını `ExecuteUpdate` ile lease'ler. Birden çok relay aynı anda çalışabilir ve her satır bir kez yayımlanır.
  Yayımlanamayan satır üstel geri çekilmeyle (en fazla 5 dk) yeniden denenir.
- **Kafka:** Idempotent producer, `acks=all`, lz4 sıkıştırma. Saga komutları **kaynak hesap id'si** ile anahtarlanır.
  Aynı hesaptan çıkan ödemeler aynı partition'da sırayla işlenir (hesap başına tek yazıcı). 24 partition farklı hesapları
  paralelleştirir.
- **Tüketici:** Process başına N thread vardır. Offset yalnızca handler başarıyla bitince saklanır (at-least-once).
  Mesaj 5 başarısız denemeden sonra DLQ'ya yazılır ve partition ilerler. DLQ'daki adımlar sweeper ile yeniden yürütülür.
- **Exactly-once etkisi** Kafka transaction'larıyla değil, handler idempotency'si ve `NoStream` garantisiyle sağlanır.

## Sonuçlar

- (+) Kaos tatbikatında processor SIGKILL, risk kesintisi ve Kafka restart altında 0 çift mutabakat, 0 kayıp ödeme
  görüldü. DLQ'ya düşen 31 komut sweeper ile tamamlandı.
- (+) Aynı hesaba yoğun trafik, çakışma fırtınası yerine sıralı işlenir.
- (−) Tek bir çok sıcak hesap (ör. büyük bir merchant'ın ödeme çıkışı) tek partition'ın hızıyla sınırlıdır. Gerekirse
  hesap alt-cüzdanlara (sub-ledger) bölünmelidir.
- (−) Polling relay veritabanına sürekli küçük bir yük bindirir. CDC (Debezium) alternatifi yol haritasındadır.
