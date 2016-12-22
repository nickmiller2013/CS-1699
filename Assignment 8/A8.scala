import java.io._

@SerialVersionUID(100L)
class RunningVar extends Serializable{
  var x: Double = 0
  var x_squared: Double = 0
  var total: Double = 0
  var variance: Double = 0

  def this(numbers:Iterator[Double]){
    this()
    numbers.foreach((this.add(_)))
  }

  def add(value: Double){
    x = x + value
    x_squared = x_squared + (value * value)
    total = total + 1
  }
  def merge(other:RunningVar):RunningVar = {
    other.x = other.x + x// XXX:
    other.x_squared = other.x_squared + x_squared
    other.total = other.total + total
    other.variance = (other.x_squared/other.total) - ((other.x/other.total)*(other.x/other.total))
    return other
  }

  def printer(){
    println(x)
    println(x_squared)
    println(total)
    println(variance)
  }
}



val intRDD = sc.parallelize(List.fill(100)(100).map(scala.util.Random.nextInt))
val doubleRDD = intRDD.map(_.toDouble)
doubleRDD.collect
println("The variance calculated by my algorithm is: " )

val myVar = doubleRDD
.mapPartitions(v=>Iterator(new RunningVar(v))) //Produces a bunch of RunningVars that are iterable
.reduce((a,b) => a.merge(b)).variance

println("The variance calculated by the built in function is: " )

val realVar = doubleRDD.variance()
