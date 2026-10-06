# ADR-0004: Deterministik kimliklerle idempotency

- **Durum:** Kabul edildi
- **Tarih:** 2026-09

## Bağlam

Mobil ağlar ve zaman aşımına uğrayan istemciler aynı ödeme isteğini tekrar gönderir. Klasik çözüm bir "idempotency
tablosu"dur: anahtar → yanıt, TTL ile. Bu yaklaşımın iki zayıf noktası var. TTL dolduktan sonra gelen tekrar ikinci
ödemeyi yaratır. Ayrıca tablo ile ödeme kaydı arasında yarış penceresi oluşur.

## Karar

- Ödeme id'si istemciden türetilir: `Deterministic.Id("payment", clientId, idempotencyKey)`. SHA-256 sonucu UUID v8
  düzenine yerleştirilir.
- Ödeme stream'i "stream yok" beklentisiyle oluşturulur. Garantiyi ayrı bir tablo değil, event store'un unique index'i
  verir.
- İsteğin alanlarından bir parmak izi hesaplanıp ödemeyle saklanır. Aynı anahtar farklı gövdeyle gelirse **422
  `Idempotency.KeyReused`** döner, sessizce eski sonuç dönmez.
- Eşzamanlı iki tekrarda kaybeden taraf `ConcurrencyConflict` yakalar, kazananı okur ve onun sonucunu döner.
- Aynı yaklaşım para yatırma/çekmede de kullanılır: işlem id'si `Deterministic.Id("Deposit" | "Withdrawal", clientId, key)`, işlem stream'i de `NoStream` ile yazılır.

## Sonuçlar

- (+) Süresiz idempotency: anahtar, ödemenin kendisi var oldukça geçerlidir.
- (+) Ek tablo, ek sorgu veya temizlik işi yoktur.
- (+) Anahtarlar istemci kapsamlıdır, farklı merchant'ların anahtarları çakışmaz.
- (−) İstemci, aynı anahtarı gerçekten farklı bir ödeme için kullanamaz (bu bir özelliktir, ama dokümante edilmelidir).
