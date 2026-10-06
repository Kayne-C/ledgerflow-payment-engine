# ADR-0002: Çift taraflı kayıt ve tamsayı minor unit

- **Durum:** Kabul edildi
- **Tarih:** 2026-09

## Bağlam

Bakiyeyi tek bir sayı olarak tutup artırıp azaltmak, nereden geldiği belli olmayan paraya kapı açar. Denetimde "bu 12,40 ₺
nereden geldi?" sorusunun cevabı olmalıdır. Ayrıca `double` kullanmak `0.1 + 0.2 ≠ 0.3` hatasını getirir. `decimal`
güvenlidir ama veritabanları ve para birimleri arasında ölçek (scale) uyuşmazlıklarına açıktır.

## Karar

- Her para hareketi bir `LedgerTransaction`'dır. Domain şu kuralları zorlar: en az 2 bacak, tüm tutarlar pozitif,
  `Σ borç = Σ alacak`.
- Tutarlar `long` minor unit olarak saklanır. Para biriminin ondalık basamak sayısı (`Currency.Exponent`) bilinir.
  Fazla ondalık **reddedilir**, sessizce yuvarlanmaz.
- Sisteme dışarıdan giren/çıkan para, para birimi başına bir **clearing hesabı** ile karşılanır. Müşteri hesapları eksiye
  düşemez, clearing hesapları düşebilir.
- İşlem stream'i (`txn-{id}`) yalnızca "stream yok" beklentisiyle yazılır. Bu, aynı işlemin iki kez deftere geçmesini
  veritabanı seviyesinde imkânsız kılar.

## Sonuçlar

- (+) `Σ müşteri + Σ clearing = 0` invariant'ı her an doğrulanabilir (`/admin/trial-balance`). Kaos tatbikatından sonra
  net 0 kuruş çıktı.
- (+) Her bakiye, yevmiye kayıtlarının toplamından yeniden üretilebilir (`/admin/reconciliation`).
- (−) Her ödeme en az 2 yevmiye satırı ve 4 stream yazması demektir. Bu, basit bir `UPDATE balance` yaklaşımından daha
  pahalıdır. Bu bilinçli bir maliyettir.
