# Drivers
Big Data Project 


## TODO
Clean up dataset (scartare dati con attributi nulli/sporchi)

Anomaly detection job

N passeggeri | fascia oraria | Trip_distance | Date[YY/YY-MM] | Trip_duration (fasce) | Fare_amount (fasce) | Total_amount (fasce) -> costo medio ($ / miles)

1 | 10-18 | 20-30 miles | 2022 | 30 - 45 minuti | 20-25 $ | 25-30$  -> 10$ / miles

 -> differenza dal costo medio

1 | Y | 1  | 100 - 125$ -> +5

RatecodeId 1 -> tot % corse prezzo medio + 5 < prezzo < prezzo medio +10
Store_and_fwd_flag Y -> tot % corse prezzo medio + 5 < prezzo < prezzo medio +10



Features bin change impact on tips

costo medio ($ / miles) (fasce) | Total_fees (Total_amount - Fare_amount - Tip_amount) (fasce) | Trip_duration (fasce) | Trip_distance (fasce) -> avg_tip

10 $ / miles | 2 - 5 $ | 30 - 40 minuti | 20 - 30 miles -> 3$
10 $ / miles | 2 - 5 $ | 30 - 40 minuti | 30 - 40 miles -> 5$


