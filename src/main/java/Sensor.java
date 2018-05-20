public class Sensor {

  private String name;

  public Sensor(String name) {
    this.name = name;
  }

  public SensorDaten produce() {
    return new SensorDaten();
  }
}
