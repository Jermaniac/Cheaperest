import org.apache.log4j._
import okhttp3.{Headers, OkHttpClient, Request, Response}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, max, min ,avg}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

case class GasStation (Provincia: String, Precio_Gasolina_95_E5: String, Precio_Gasolina_98_E5: String, Municipio: String, Localidad: String, IDEESS: String )

object Cheaperest {

  val datosGasGovUri = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/"

  def ExecuteHttpGet(url: String) : Option[String] = {

    val client: OkHttpClient = new OkHttpClient();

    val headerBuilder = new Headers.Builder
    val headers = headerBuilder
      .add("content-type", "application/json")
      .build

    val result = try {
      val request = new Request.Builder()
        .url(url)
        .headers(headers)
        .build();

      val response: Response = client.newCall(request).execute()
      response.body().string()
    }
    catch {
      case e: Throwable =>  {print(e);  null}
    }
    Option[String](result)
  }

  def main(args: Array[String]): Unit = {
    // set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("Cheaperest")
      .master("local[*]")
      .getOrCreate()

    val gasStationSchema = new StructType()
      .add("Provincia", StringType, true)
      .add("Precio Gasolina 95 E5", StringType, true)
      .add("Precio Gasolina 98 E5", StringType, true)
      .add("Municipio", StringType, true)
      .add("Localidad", StringType, true)
      .add("IDEESS", StringType, true)

    val restApiSchema = new StructType()
      .add("Fecha", StringType,true)
      .add("ListaEESSPrecio",
        ArrayType(gasStationSchema)
        ,true)
      .add("Nota", StringType,true)
      .add("ResultadoConsulta", StringType,true)

    // get string json
    val restApiString = ExecuteHttpGet(datosGasGovUri).getOrElse("")

    // print(restApiString)
    import spark.implicits._
    val datosGasGovDF = spark.read.schema(restApiSchema).json(Seq(restApiString).toDS()).repartition(6)
    println(datosGasGovDF.rdd.getNumPartitions)
    println(datosGasGovDF.rdd.count())

    val list_of_gas_station = datosGasGovDF.select(explode(col("ListaEESSPrecio")).alias("gasStation"))
      .select(col("gasStation.Provincia"), col("gasStation.Municipio"), col("gasStation.Localidad"),
        col("gasStation.Precio Gasolina 95 E5"), col("gasStation.Precio Gasolina 98 E5"),col("gasStation.IDEESS"))
      .withColumnRenamed("Precio Gasolina 95 E5","Precio_Gasolina_95_E5")
      .withColumnRenamed("Precio Gasolina 98 E5","Precio_Gasolina_98_E5")
      .as[GasStation]
  .filter(col("IDEESS").equalTo("4708"))
      .cache()

    list_of_gas_station.show()
    val the_highest_price = list_of_gas_station.filter($"Precio_Gasolina_95_E5".notEqual(""))
     .agg(max(col("Precio_Gasolina_95_E5"))).show()

    val the_lowest_price = list_of_gas_station.filter($"Precio_Gasolina_95_E5".notEqual(""))
      .agg(min(col("Precio_Gasolina_95_E5"))).show()

    val the_average_price = list_of_gas_station.filter($"Precio_Gasolina_95_E5".notEqual(""))
      .agg(avg(col("Precio_Gasolina_95_E5"))).show()

  }
}