package res;

public enum TaskStatus {
    Run, Success, Fail;

    public boolean isFail() {
        return true;
    }

    public boolean isSuccess() {
        return true;
    }
}
