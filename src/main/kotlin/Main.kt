import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlin.random.Random

// ─────────────────────────────────────────────────────────────
// 1) Data model
// ─────────────────────────────────────────────────────────────
data class Order(
    val id: String,
    val dishName: String,
    var cookingProgress: Int = 0,
    var status: String = "Pending"
)

data class DishResult(
    val orderId: String,
    val dishName: String,
    val qualityScore: Double
)

// ─────────────────────────────────────────────────────────────
// 2) Ingredient preparation — Dispatchers.Default (computation)
// ─────────────────────────────────────────────────────────────
suspend fun prepareIngredient(ingredientName: String): String =
    withContext(Dispatchers.Default) {
        delay(1500)
        when (ingredientName) {
            "onion"   -> "Chopped onion"
            "butter"  -> "Melted butter"
            "cream"   -> "Whipped cream"
            else      -> "Prepared $ingredientName"
        }
    }

// ─────────────────────────────────────────────────────────────
// 3) Parallel order cooking via coroutineScope + launch
// ─────────────────────────────────────────────────────────────
suspend fun cookOrder(order: Order): Unit = supervisorScope {
    order.status = "Cooking"

    // 3.1 Cooking: every 500 ms +10%
    launch {
        while (isActive && order.cookingProgress < 100) {
            delay(500)
            if (!isActive) break
            order.cookingProgress += 10
            println("  [${order.id}] ${order.dishName}: progress ${order.cookingProgress}%")
        }
        if (isActive) {
            order.status = "Ready"
            println("  [${order.id}] ${order.dishName} ready for pickup")
        }
    }

    // 3.2 Temperature check: every 2 sec
    launch {
        while (isActive && order.cookingProgress < 100) {
            delay(2000)
            if (!isActive) break
            println("  [${order.id}] ${order.dishName}: temperature normal (probe)")
        }
    }

    // 3.3 Progress monitoring: warning at 80%
    launch {
        var warned = false
        while (isActive && order.cookingProgress < 100) {
            delay(200)
            if (!isActive) break
            if (!warned && order.cookingProgress >= 80) {
                warned = true
                println("  [${order.id}] Dish almost ready, time for final processing!")
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────
// 4) async / await — chef's tasting
// ─────────────────────────────────────────────────────────────
suspend fun checkDishQuality(
    orderId: String,
    dishName: String = "Dish"
): Deferred<DishResult> =
    CoroutineScope(Dispatchers.Default).async {
        delay(3000)                                          // chef tastes for 3 seconds
        val score = 7.0 + Random.nextDouble(0.0, 3.0)        // 7.0 … 10.0
        DishResult(orderId, dishName, score)
    }

// ─────────────────────────────────────────────────────────────
// 5) Order cancellation
// ─────────────────────────────────────────────────────────────
suspend fun cancelOrder(order: Order, job: Job) {
    delay(2000)                                              // "2 seconds after start"
    job.cancelAndJoin()                                      // cancel and wait for completion
    order.status = "Cancelled"
    println("  Order ${order.id} cancelled, dish ${order.dishName} removed from preparation")
}

// ─────────────────────────────────────────────────────────────
// 6) Channel "kitchen → serving"
// ─────────────────────────────────────────────────────────────
suspend fun kitchenToWaiterDemo(): Unit = coroutineScope {
    val channel = Channel<DishResult>()

    // Producer — kitchen
    launch {
        val dishes = listOf(
            DishResult("Z1", "Risotto",   9.2),
            DishResult("Z2", "Sous-vide", 8.7),
            DishResult("Z3", "Steak",     9.5),
            DishResult("Z4", "Puree soup",8.9),
            DishResult("Z5", "Pasta",     9.1)
        )
        for (d in dishes) {
            delay(300)
            channel.send(d)
            println("  Kitchen → ${d.dishName} (order ${d.orderId})")
        }
        channel.close()
        println("  Channel closed — shift finished")
    }

    // Consumer — maître d'
    launch {
        for (result in channel) {
            delay(200)
            println("  Waiter served: ${result.dishName} " +
                    "(order ${result.orderId}, quality ${"%.1f".format(result.qualityScore)})")
        }
    }
}

// ─────────────────────────────────────────────────────────────
// 7) Exception handling: SupervisorJob + CoroutineExceptionHandler
// ─────────────────────────────────────────────────────────────
suspend fun exceptionHandlingDemo(): Unit = supervisorScope {
    val handler = CoroutineExceptionHandler { _, e ->
        println("  Exception handled: ${e.message}")
    }

    // "Problem" dish — burns
    launch(handler) {
        val burnt = Order("BURN-1", "Steak")
        println("  [${burnt.id}] ${burnt.dishName} on the grill...")
        delay(800)
        throw IllegalStateException("Steak burnt! Dish written off")
    }

    // Neighboring dish continues cooking
    launch {
        val safe = Order("SAFE-1", "Risotto")
        while (safe.cookingProgress < 100) {
            delay(400)
            safe.cookingProgress += 20
            println("  [${safe.id}] ${safe.dishName}: ${safe.cookingProgress}%")
        }
        println("  [${safe.id}] Risotto ready — neighbor's failure didn't stop the shift")
    }
}

// ─────────────────────────────────────────────────────────────
// MAIN — sequential demonstration of all tasks
// ─────────────────────────────────────────────────────────────
fun main() = runBlocking {
    val line = "═".repeat(64)
    println(line)
    println("  RESTAURANT \"PARALLEL\" — shift start")
    println(line)

    // ── Task 2 ───────────────────────────────────────────────
    println("\n▶ Task 2. Ingredient preparation (Dispatchers.Default)")
    val ingredients = listOf("onion", "butter", "cream")
    val prepared = ingredients.map { async { prepareIngredient(it) } }.awaitAll()
    prepared.forEach { println("  $it") }

    // ── Task 3 ───────────────────────────────────────────────
    println("\n▶ Task 3. Parallel order cooking")
    val order1 = Order("ORD-1", "Risotto")
    cookOrder(order1)

    // ── Task 4 ───────────────────────────────────────────────
    println("\n▶ Task 4. Chef's tasting (async / await)")
    val result = checkDishQuality("ORD-1", "Risotto").await()
    println("  Chef rated \"${result.dishName}\": ${"%.1f".format(result.qualityScore)}/10")

    // ── Task 5 ───────────────────────────────────────────────
    println("\n▶ Task 5. Order cancellation (job.cancel)")
    val orderToCancel = Order("ORD-2", "Steak")
    coroutineScope {
        val job = launch(Dispatchers.Default) {
            try {
                cookOrder(orderToCancel)
            } finally {
                println("  Resources of order ${orderToCancel.id} released (finally executed)")
            }
        }
        cancelOrder(orderToCancel, job)
    }
    println("  Final status of ${orderToCancel.id}: ${orderToCancel.status}, " +
            "progress: ${orderToCancel.cookingProgress}%")

    // ── Task 6 ───────────────────────────────────────────────
    println("\n▶ Task 6. Channel \"kitchen → serving\"")
    kitchenToWaiterDemo()

    // ── Task 7 ───────────────────────────────────────────────
    println("\n▶ Task 7. Exception handling (SupervisorJob)")
    exceptionHandlingDemo()

    println("\n$line")
    println("  Shift finished. Chef Arsen is pleased")
    println(line)
}