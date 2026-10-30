import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.flow.*

// ─────────────────────────────────────────────────────────────
// МОДЕЛЬ
// ─────────────────────────────────────────────────────────────
data class SensorReading(
    val sensorId: String,
    val value: Double,
    val timestamp: Long
)

// ─────────────────────────────────────────────────────────────
// ЧАСТЬ 1. КОРУТИНЫ + CHANNEL (push-модель, ручная обработка)
// ─────────────────────────────────────────────────────────────

/**
 * В ТЗ сигнатура без sensorId, но тогда 3 корутины неразличимы.
 * Добавил параметр — это единственное отступление от ТЗ.
 */
suspend fun emitSensorData(
    channel: SendChannel<SensorReading>,
    sensorId: String
) = coroutineScope {
    launch {
        repeat(10) { i ->
            delay(500)
            channel.send(
                SensorReading(
                    sensorId = sensorId,
                    value = simulateValue(sensorId, i),
                    timestamp = System.currentTimeMillis()
                )
            )
        }
    }
}

private fun simulateValue(sensorId: String, i: Int): Double = when (sensorId) {
    "TEMP" -> 19.0 + i * 1.0     // 19..28 °C
    "HUM"  -> 35.0 + i * 2.0     // 35..53 %
    "CO2"  -> 400.0 + i * 20.0   // 400..580 ppm
    else   -> 20.0 + i
}

// ─────────────────────────────────────────────────────────────
// ЧАСТЬ 2. FLOW (pull-модель, ленивый холодный поток)
// ─────────────────────────────────────────────────────────────
fun sensorFlow(sensorId: String): Flow<SensorReading> = flow {
    println("  [flow:$sensorId] старт (поток был холодным до терминальной операции)")
    repeat(10) { i ->
        delay(300)
        emit(
            SensorReading(
                sensorId = sensorId,
                value = 15.0 + i * 1.5,   // 15..28.5 — пересекает порог 20
                timestamp = System.currentTimeMillis()
            )
        )
    }
}

// ─────────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────────
fun main() = runBlocking {

    // ═══════════ ДЕМО 1. Channel + корутины ═══════════
    println("=".repeat(64))
    println("1) КОРУТИНЫ + CHANNEL — ручной цикл for (msg in channel)")
    println("=".repeat(64))

    val channel = Channel<SensorReading>(capacity = Channel.UNLIMITED)
    val sensors = listOf("TEMP", "HUM", "CO2")

    coroutineScope {
        val producers = sensors.map { id ->
            launch { emitSensorData(channel, id) }
        }
        // отдельная корутина закрывает канал, когда все продюсеры закончили
        launch {
            producers.forEach { it.join() }
            channel.close()
        }

        var n = 0
        for (msg in channel) {          // ← ручной pull, каждая строка = ручная обработка
            n++
            println("  [$n] $msg")
        }
        println("→ Получено $n показаний\n")
    }

    // ═══════════ ДЕМО 2. Flow — декларативная цепочка ═══════════
    println("=".repeat(64))
    println("2) FLOW — filter → map → take(5) → onEach, ленивый")
    println("=".repeat(64))

    val pipeline = sensorFlow("TEMP")
        .filter { it.value > 20.0 }                       // отсекаем холодное
        .map    { it.copy(value = it.value * 1.1) }       // калибровка
        .take(5)                                          // первые 5 годных
        .onEach { println("  Обработано: $it") }

    println("  Цепочка объявлена. Ждём 1 сек — и ничего не происходит:")
    delay(1000)
    println("  ↑ выше ни строчки — доказательство ленивости.")
    println("  Вызываем терминальный toList():")
    val collected = pipeline.toList()
    println("→ Собрано ${collected.size} элементов (take(5) сам отменил поток)\n")

    // ═══════════ ДЕМО 3. fold — сумма и среднее ═══════════
    println("=".repeat(64))
    println("3) FOLD — сумма и среднее через аккумулятор")
    println("=".repeat(64))

    val (sum, count) = sensorFlow("TEMP")
        .filter { it.value > 20.0 }
        .fold(0.0 to 0) { (s, c), r -> (s + r.value) to (c + 1) }

    println("  Сумма:   $sum")
    println("  Кол-во:  $count")
    println("  Среднее: ${if (count > 0) sum / count else 0.0}")
}

