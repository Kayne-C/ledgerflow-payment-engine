# ADR-0003: Bloke (hold) kullanan orkestrasyonlu saga

- **Durum:** Kabul edildi
- **Tarih:** 2026-09

## Bağlam

Bir ödeme, defterin dışında bir karara (risk servisi, gRPC) bağlıdır ve bu karar saniyeler sürebilir ya da servis hiç
cevap vermeyebilir. Bu sırada (1) aynı bakiye başka bir ödemeye harcanmamalı, (2) para hiçbir ara durumda kaybolmamalı,
(3) bir adım iki kez çalışırsa sonuç değişmemelidir.

## Seçenekler

1. **2PC / dağıtık transaction**: risk servisi bir veritabanı değil. Ayrıca kilitleri saniyelerce tutmak ölçeklenmez.
2. **Koreografi** (her servis bir olaya tepki verir): akış birden çok yere dağılır. "Bu ödeme şu an nerede?" sorusunun tek
   bir cevabı olmaz, telafi mantığı da dağınık kalır.
3. **Orkestrasyon + hold (seçilen)**: durum `Payment` aggregate'inde tutulur. Her adım bir sonraki adımın komutunu
   outbox'a yazar. Para önce bloke edilir, onaydan sonra capture edilir, retten sonra serbest bırakılır.

## Karar

`Initiated → FundsReserved → RiskApproved/RiskRejected → Completed/Failed` durum makinesi kullanılır. Her handler önce
durumu kontrol eder. Beklenmeyen bir durumda `AlreadyHandled` döner, bu yüzden tekrar gelen mesaj zararsızdır. Risk
kararı bir olay olarak kaydedilir, replay'de dış servis tekrar çağrılmaz. `StalledPaymentSweeper` ilerlemeyen
ödemelerin sonraki adımını yeniden yayımlar ve 30 dakikayı aşanları zaman aşımıyla telafi eder.

## Sonuçlar

- (+) Risk servisi kesintisinde para kaybolmaz, bloke halinde bekler. Servis dönünce saga kaldığı yerden devam eder
  (test: `A_risk_engine_outage_keeps_funds_reserved_and_the_saga_resumes_after_recovery`).
- (+) Telafi tek bir yerde: `ReleasePaymentFunds`.
- (−) Ödeme uçtan uca asenkrondur. API 202 döner, istemci durumu sorgular (veya ileride webhook alır).
- (−) Bloke, kullanılabilir bakiyeyi geçici olarak düşürür. Zaman aşımı süresi iş tarafıyla birlikte belirlenmelidir.
