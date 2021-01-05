package runnable

abstract class Runnable() {
  // Default Arguments must be overridden by child classes
  def execute(firstQuery: Int = 0, lastQuery: Int = 0): Long 
}
