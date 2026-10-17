# Cómo está armado

Imaginátelo como el cuaderno de la bodega de Ruta Sur Logística, una empresa ficticia. Cada vez que un envío cambia de estado, alguien anota una línea con un número de secuencia (el LSN). El consumidor lee esas líneas en orden por clave, y si la misma línea llega dos veces no vuelve a pintar el estado.

## Recorrido

1. `python -m cdc_lab generate` arma eventos sintéticos: altas, cambios, bajas y lecturas de snapshot, más fallas a propósito (duplicados, desorden, JSON roto, esquema, versión desconocida, payload grande).
2. El broker en memoria los parte por clave con SHA-256 (no usa el hash de Python, que cambia de proceso).
3. El consumidor decodifica. Si el mensaje es veneno, va a la DLQ con los bytes originales y se commitea el offset para no trabar a las otras claves.
4. Si el downstream falla de forma transitoria, reintenta con jitter completo y un tope. Si se agotan los intentos, DLQ.
5. El efecto y la marca de deduplicación se escriben en la misma transacción de SQLite (o en el store en memoria). El offset se commitea después. Si el proceso muere en el medio, al reiniciar el efecto queda una sola vez.
6. Un LSN más viejo que el que ya aplicaste se cuenta como `skipped_stale` y no pisa el estado.
7. Recién ahí el agente de mentira mira reglas: cliente premium que pasa a demorado, o una fecha prometida que se corre más de N días. Deja un borrador. No manda nada. Un humano tiene que aprobar.

## Glosario

| Palabra | Qué es acá |
| --- | --- |
| LSN | Número de secuencia del cambio. Gana el más alto por clave. |
| Offset | Posición en la partición. Distinto del `event_id`. |
| DLQ | Cola de mensajes que no se pueden aplicar. Conserva el payload original. |
| Efectivamente una vez | Al menos una entrega, más idempotencia del efecto. |
| Cassette | Respuesta fija del “modelo”. Si la clave no está, es un error. No hay red. |
| Reloj simulado | Un contador que avanza cuando el lab lo dice. No es un reloj de pared. |

## Límites, en serio

- No hay Debezium corriendo, ni Postgres de verdad, ni Kafka en la medición.
- El agente no llama a un modelo. Los tokens del cassette son 0 porque el texto está escrito en el repo.
- La latencia que ves en el histograma es tiempo simulado.
- El `bench` es una sola muestra en memoria, con logs JSON a stderr. No es un benchmark de Kafka.
- Si el downstream manda un mail o cobra, el replay deja de ser seguro. Acá el efecto es escribir estado.

## Guion de dos minutos

1. `python -m cdc_lab demo` y mostrá que `applied + skipped_stale + duplicates_skipped + dlq` cierra con `produced`.
2. `python -m cdc_lab eval` y leé `violations: 0`.
3. `python -m cdc_lab eval --regression` y mostrá que sale con código 1: sin idempotencia el mismo evento se aplica cinco veces.
4. Cerrá diciendo que los borradores quedan `pending` y que el kill switch (`CDC_AGENT_ENABLED=0`) sigue consumiendo pero no invoca al cassette.
