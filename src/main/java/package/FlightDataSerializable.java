package bmstu;

import java.io.Serializable;

public class FlightDataSerializable implements Serializable {

    private double maxDelay;
    private int delayedCount;
    private int allFlightsCount;

    public FlightDataSerializable(double maxDelay, int delayedCount, int allFlightsCount) {
        this.maxDelay = maxDelay;
        this.delayedCount = delayedCount;
        this.allFlightsCount = allFlightsCount;
    }

    public void DelyedAdd(){
        delayedCount++;
        allFlightsCount++;
    }
    public void AllAdd(){
        allFlightsCount++;
    }

    public static FlightDataSerializable addValue(FlightDataSerializable a , AiroportDataSeriazable b){
        int isDelayed = 0;
        if (b.getTimeDelay() > 0 || b.isCanceld())
            isDelayed = 1;
        return new FlightDataSerializable(
                Math.max(a.getMaxDelay() ,b.getTimeDelay()),
                a.getDelayedCount() + isDelayed,
                a.getAllFlightsCount() + 1
        );
    }

    public static FlightDataSerializable Add(FlightDataSerializable a , FlightDataSerializable b){
        return new FlightDataSerializable(
                Math.max(a.maxDelay , b.maxDelay),
                a.getDelayedCount() + b.getDelayedCount(),
                a.getAllFlightsCount() + b.getAllFlightsCount()
        );
    }
    public double ReturnProcent() {
        if (delayedCount == 0)
            return 0;
        double res = (double)delayedCount / (double)allFlightsCount * 100;
        return res;
    }
    public void MaxDelayCompare(double newDelay){
        maxDelay = Math.max(maxDelay , newDelay);
    }

    public double getMaxDelay() {
        return maxDelay;
    }

    public void setMaxDelay(double maxDelay) {
        this.maxDelay = maxDelay;
    }

    public int getDelayedCount() {
        return delayedCount;
    }

    public void setDelayedCount(int delayedCount) {
        this.delayedCount = delayedCount;
    }

    public int getAllFlightsCount() {
        return allFlightsCount;
    }

    public void setAllFlightsCount(int allFlightsCount) {
        this.allFlightsCount = allFlightsCount;
    }
}
