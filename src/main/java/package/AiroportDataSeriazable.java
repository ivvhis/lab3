package bmstu;

import java.io.Serializable;

public class AiroportDataSeriazable implements Serializable {
    private int originAiroportID;
    private int destAiroportID;
    private double timeDelay;
    private boolean isCanceld;
    public AiroportDataSeriazable(){

    }
    public AiroportDataSeriazable(int originAiroportID, int destAiroportID, double timeDelay, boolean isCanceld) {
        this.originAiroportID = originAiroportID;
        this.destAiroportID = destAiroportID;
        this.timeDelay = timeDelay;
        this.isCanceld = isCanceld;
    }

    public double getTimeDelay() {
        return timeDelay;
    }

    public void setTimeDelay(int timeDelay) {
        this.timeDelay = timeDelay;
    }

    public boolean isCanceld() {
        return isCanceld;
    }

    public void setCanceld(boolean canceld) {
        isCanceld = canceld;
    }

    public int getDestAiroportID() {
        return destAiroportID;
    }

    public void setDestAiroportID(int destAiroportID) {
        this.destAiroportID = destAiroportID;
    }

    public int getOriginAiroportID() {
        return originAiroportID;
    }

    public void setOriginAiroportID(int originAiroportID) {
        this.originAiroportID = originAiroportID;
    }
}
