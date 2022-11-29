import fon_query
import datetime

import seaborn
import matplotlib.pyplot as plt

conn = fon_query.connect_db()

#fon_query.plot_prices(conn, ["MAC","ZDZ","TI2"], normalize=True)


#   içindeki "stock" oranı %90'dan yüksek olan fonların listesini çıkar
#res = fon_query.get_fon_codes_with_min_attribute(conn, "stock", 90)
#   bu listedeki fonlar ın fiyat grafiğini çiz. normalize = True olduğu için fiyatları başlangıç fiyatına göre oranla
#fon_query.plot_prices(conn, res, normalize=True)


#   içindeki "stock" oranı %90'dan yüksek olan fonların listesini çıkar
#res = fon_query.get_fon_codes_with_min_attribute(conn, "stock", 90)
#   bu listedeki fonlar için son 10 gündeki kazanç oranlarını hesapla
#s = fon_query.get_change(conn, res, 10)
#for i in range(s.shape[0]):
#    print(s["code"][i], s["change"][i])
#print(s)

s = fon_query.get_foncodes_with_keyword_in_fontitle(conn, "gümüş")

for c in s:
    r = fon_query.get_prices_between_dates(conn, c, "2022-10-01", "2022-11-25", normalize=True)
    seaborn.lineplot(data=r, x="date", y="price", label=r["code"][0])

plt.show()
#for f in s:
#    d = fon_query.get_prices_df(conn, f)
#    print(d)
#    fon_query.plot_prices(conn, [f])