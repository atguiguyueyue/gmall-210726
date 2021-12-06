import com.alibaba.fastjson.JSON

object test {
  def main(args: Array[String]): Unit = {

    val orderInfo: OrderInfo = new OrderInfo("101","zs")

//    println(JSON.toJSONString(orderInfo))

  }

  case class OrderInfo(
                        id: String,
                       name: String)

}
