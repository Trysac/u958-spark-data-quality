package io.github.trysac.spark_data_quality

import scala.util.Random

/** Deterministic generator (seed 958) building the 10,000-row orders dataset with seeded anomalies.
  */
object SeededOrdersGenerator {

  /** Schema of the orders dataset: `order_id`, `email`, `amount`, `country`. */
  val OrdersSchema: Schema = Schema(
    Vector(
      "order_id" -> ColumnType.Text,
      "email" -> ColumnType.Text,
      "amount" -> ColumnType.Float64,
      "country" -> ColumnType.Text,
    ),
  )

  /** Valid EU country codes expected by the orders check. */
  val ValidCountries: Set[String] = Set("ES", "FR", "DE", "IT", "PT")

  private val ValidCountryVec: Vector[String] = Vector("ES", "FR", "DE", "IT", "PT")
  private val InvalidCountryVec: Vector[String] = Vector("US", "GB", "BR", "JP")

  /** Generates 10,000 rows using `seed = 958`.
    *
    * When `injectAnomalies` is `true`, injects:
    *   - exactly 37 null `email` values
    *   - 5 `order_id` values that each appear twice (10 rows total out of 10,000 non-null)
    *   - 3 negative `amount` values (`-0.01`, `-5.00`, `-120.50`)
    *   - 12 `country` values outside `"ES", "FR", "DE", "IT", "PT"`
    */
  def generate(
      seed: Long = 958L,
      totalRows: Int = 10_000,
      injectAnomalies: Boolean = true,
  ): Vector[Vector[Cell]] = {
    val rng = new Random(seed)
    val allIndices = rng.shuffle((0 until totalRows).toVector)

    val nullEmailIndices: Set[Int] =
      if (injectAnomalies) allIndices.slice(0, 37).toSet else Set.empty
    val duplicateTargetIndices: Map[Int, Int] =
      if (injectAnomalies) {
        val sources = allIndices.slice(37, 42)
        val targets = allIndices.slice(42, 47)
        targets.zip(sources).toMap
      } else Map.empty
    val negativeAmountsByIndex: Map[Int, Double] =
      if (injectAnomalies) {
        Map(
          allIndices(47) -> -0.01,
          allIndices(48) -> -5.00,
          allIndices(49) -> -120.50,
        )
      } else Map.empty
    val invalidCountryIndices: Set[Int] =
      if (injectAnomalies) allIndices.slice(50, 62).toSet else Set.empty

    (0 until totalRows).toVector.map { idx =>
      val orderIdIdx = duplicateTargetIndices.getOrElse(idx, idx)
      val orderIdCell = Cell.Text(f"ORD-$orderIdIdx%06d")

      val emailCell =
        if (nullEmailIndices.contains(idx)) Cell.Null
        else Cell.Text(s"customer$idx@example.com")

      val positiveAmount = 10.0 + (rng.nextInt(50_000).toDouble / 100.0)
      val amountCell = Cell.Float64(negativeAmountsByIndex.getOrElse(idx, positiveAmount))

      val countryCell =
        if (invalidCountryIndices.contains(idx)) {
          Cell.Text(InvalidCountryVec(idx % InvalidCountryVec.length))
        } else {
          Cell.Text(ValidCountryVec(rng.nextInt(ValidCountryVec.length)))
        }

      Vector(orderIdCell, emailCell, amountCell, countryCell)
    }
  }
}
