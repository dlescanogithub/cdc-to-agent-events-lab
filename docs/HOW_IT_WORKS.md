# Cómo está armado

Este texto es para contarlo en una entrevista, en voz alta. La empresa, Ruta Sur Logística, es ficticia. El "modelo" es una cassette. No hay una base de datos real atrás. Los números de negocio en dólares están en el README, en inglés, y son [EST] o cero. Acá no invento otros.

## La analogía

Imaginátelo como el cuaderno de la bodega. Cada vez que un envío cambia, alguien anota una línea con un número de secuencia (el LSN). El consumidor lee esas líneas. Si la misma línea llega dos veces, no vuelve a pintar el estado. Si llega una línea más vieja que la que ya aplicó, la cuenta como vieja y no pisa el estado nuevo. Si la línea está rota, la aparta en una bandeja (la DLQ) y sigue con las otras claves. Recién después, un empleado de mentira mira si el cliente es premium y el envío pasó a demorado, o si la fecha prometida se corrió más de N días. Deja un borrador. No lo manda. Una persona tiene que aprobarlo.

El reloj del laboratorio es un contador. Avanza cuando el código lo dice. No es la hora de la pared.

## Recorrido

1. `python -m cdc_lab generate` arma eventos sintéticos: altas, cambios, bajas y lecturas de snapshot, más fallas a propósito (duplicados, desorden, JSON roto, esquema, versión desconocida, payload grande).
2. El broker en memoria reparte por clave con SHA-256 (no usa el hash de Python, que cambia de proceso). Un commit avanza el offset como máximo uno, y no puede volver atrás. Si no hay ack, el mismo offset reaparece cuando vence el lease de visibilidad.
3. El consumidor decodifica. Si el mensaje es veneno, va a la DLQ con los bytes originales y se commitea el offset para no trabar a las otras claves.
4. Si el downstream falla de forma transitoria, reintenta con jitter completo y un tope. El jitter sale de un RNG con semilla. Si se agotan los intentos, DLQ.
5. El efecto y la marca de deduplicación se escriben en la misma transacción de SQLite (o en el store en memoria). El offset se commitea después.
6. Si el proceso muere antes de commitear el store (`before_store_commit`), la transacción se deshace y el reintento aplica una sola vez. Si muere después del store y antes del offset (`after_store_before_offset`), al reiniciar el efecto ya está y no se aplica de nuevo. El escenario `crash_after_effect` hace eso una vez: 5 eventos, cada efecto queda en 1.
7. Un LSN más viejo que el que ya aplicaste se cuenta como `skipped_stale` y no pisa el estado.
8. Recién ahí el agente de mentira mira reglas. Cliente premium que pasa a demorado, o una fecha prometida que se corre **más** de N días (el default de `CDC_DATE_SLIP_DAYS` es 2; la comparación es estricta, no "2 o más"). Deja un borrador `pending`. No manda nada. `send` tira `NotApproved` si no hay un nombre de aprobador. Con `CDC_AGENT_ENABLED=0` sigue consumiendo, marca el outbox como `suppressed` y no llama a la cassette.
9. Varios avisos del mismo envío dentro de la ventana de coalesce (default 5000 ms) se juntan. La ventana es por clave de envío, no por todo el lote. El breaker abre después de K fallos seguidos, deja las filas en el outbox y puede cerrar después del cooldown.

Las dos frases de la cassette están en español en `src/cdc_lab/cassette.py`. Dicen que el aviso está pendiente de aprobación y que no se envió. Los tokens son 0 porque el texto ya está en el repo. Si la clave no está en la cassette, es un error. No hay red.

## Dos "mixed" distintos

Los dos se llaman `mixed_faults` y los dos usan semilla 7 y 80 eventos. No son el mismo mix.

- El archivo `scenarios/mixed_faults.yaml` pide 8 duplicados y 7 venenos. La tabla del README (y de `docs/METRICS.md`) es applied 32, stale 33, duplicates 8, DLQ 7. 32 + 33 + 8 + 7 = 80. El 33 de stale no es el 4 de swaps del archivo: son contadores distintos.
- `python -m cdc_lab demo` arma otro mix en el CLI (3 duplicados, 1 malformado, 1 violación de esquema). El 2026-10-22 imprimió applied 59, stale 16, duplicates 3, DLQ 2, violations 0, `drafts_pending` 10, y `clock: simulated`. 59 + 16 + 3 + 2 = 80. Esos 10 borradores no son los 4 del caso de negocio.

El caso de negocio usa `agent_breaker`: 4 borradores, todos `pending`. Eso se volvió a correr el 2026-10-22 y dio otra vez 4. El precio de 48 USD es [EST] (4 × 12) y está explicado en inglés en `docs/BUSINESS_VALUE.md`. No es una medición de un contact center.

## Qué está medido

El detalle y la fecha de cada comando están en el README, sección Proof, y en `docs/METRICS.md`. En corto, el 2026-10-22, Python 3.12.3, paquete 0.1.0:

- `python3 -m pytest --cov=cdc_lab --cov-report=term-missing --cov-fail-under=80`: 68 passed, cobertura de líneas 95.55% (la línea `Total coverage` que imprime pytest-cov). pytest 9.1.1.
- `python3 -m cdc_lab eval`: 0 violaciones, 10 escenarios, salida 0. El mismo texto ya estaba anotado el 2026-10-17.
- `python3 -m cdc_lab eval --regression`: salida 1. Sin idempotencia, `evt-000001` se aplica 5 veces.
- `python3 -m cdc_lab.secretscan`: sin hallazgos.

El bench de 2000 eventos (17.276627 s, 115.76 eventos/s, una muestra, en memoria) es del 2026-10-17. No se volvió a correr el 2026-10-22. No es un benchmark de Kafka.

Las ocho invariantes, en una frase: no se pierde ningún evento, cada efecto entra una vez, gana el LSN más alto, los reintentos tienen tope, el veneno no traba a las otras claves, el lag termina en 0 o hay alerta, el agente no se pasa del presupuesto ni manda sin aprobación, y el replay aplica una vez lo que se puede reparar. Reintentos, veneno y replay no están ejercidos en los 10 escenarios: el gate lo dice con `non_vacuous` (1, 4 y 2).

## Glosario

| Palabra | Qué es acá |
| --- | --- |
| LSN | Número de secuencia del cambio. Gana el más alto por clave. |
| Offset | Posición en la partición. Distinto del `event_id`. |
| DLQ | Cola de mensajes que no se pueden aplicar. Conserva el payload original. |
| Efectivamente una vez | Al menos una entrega, más idempotencia del efecto. No es una transacción de Kafka. |
| Cassette | Respuesta fija. Si la clave no está, es un error. No hay red. |
| Reloj simulado | Un contador que avanza cuando el lab lo dice. No es un reloj de pared. |
| Borrador `pending` | Texto que el agente dejó. No salió. Hace falta un nombre en `approve` para que `send` no falle. |
| Kill switch | `CDC_AGENT_ENABLED=0`. Sigue el consumo. No llama a la cassette. |

## Límites, en serio

- No hay Debezium corriendo, ni Postgres de verdad, ni Kafka en la medición. El compose y el Dockerfile existen y CI no los arranca.
- El agente no llama a un modelo. Los tokens de la cassette son 0.
- La latencia del histograma es tiempo simulado. El pico de 20 registros en `lag_drain` es de ese reloj, con umbral 5 en ese archivo, no el default de 1000.
- El `bench` es una sola muestra en memoria, con logs JSON a stderr. No es un benchmark de Kafka.
- Si el downstream manda un mail o cobra, el replay deja de ser seguro. Acá el efecto es escribir estado.
- El CLI de replay no anota el nombre de quien aprobó. `approve` de un borrador acepta cualquier nombre no vacío. No exige una segunda persona.
- Medido con proveedores reales: todavía ninguno.
- El ROI en porcentaje no está calculado. El costo medido del camino default es 0 USD, y no hay una cotización de cluster. Eso está en `docs/BUSINESS_VALUE.md`.

## Guion de dos minutos

1. `python -m cdc_lab demo` y mostrá que applied + stale + duplicates + dlq cierra con produced. Decí que ese mix no es el YAML.
2. `python -m cdc_lab eval` y leé `violations: 0`.
3. `python -m cdc_lab eval --regression` y mostrá que sale con código 1: sin idempotencia el mismo evento se aplica cinco veces.
4. Cerrá diciendo que los borradores quedan `pending`, que el kill switch sigue consumiendo pero no invoca a la cassette, y que el bench no es Kafka.
