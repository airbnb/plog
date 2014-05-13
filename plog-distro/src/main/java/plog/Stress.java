package plog;

import com.airbnb.plog.stress.PlogStress;

import java.net.SocketException;

@SuppressWarnings("ClassOnlyUsedInOneModule")
public final class Stress {
    public static void main(String[] args) throws SocketException {
        PlogStress.main(args);
    }
}
