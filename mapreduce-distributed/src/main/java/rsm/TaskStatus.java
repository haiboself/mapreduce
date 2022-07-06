package rsm;

public enum TaskStatus {
    Run, Success, Fail;

    public boolean isFail() {
        return this == Fail;
    }

    public boolean isSuccess() {
        return this == Success;
    }

    public boolean isRunning() {
        return this == Run;
    }
}
