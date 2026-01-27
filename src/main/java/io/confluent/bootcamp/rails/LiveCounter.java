package io.confluent.bootcamp.rails;

import com.googlecode.lanterna.graphics.TextGraphics;
import com.googlecode.lanterna.screen.Screen;
import com.googlecode.lanterna.screen.TerminalScreen;
import com.googlecode.lanterna.terminal.DefaultTerminalFactory;
import com.googlecode.lanterna.terminal.Terminal;
import com.googlecode.lanterna.terminal.ansi.UnixLikeTerminal;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class LiveCounter {
    // Atomic variables are best for thread-safe counters in Java 25
    private static final AtomicLong counter = new AtomicLong(0);
    private static final AtomicLong other = new AtomicLong(100);

    static void main() throws Exception {
        DefaultTerminalFactory factory = new DefaultTerminalFactory();
        factory.setUnixTerminalCtrlCBehaviour(UnixLikeTerminal.CtrlCBehaviour.CTRL_C_KILLS_APPLICATION);

        Terminal terminal = factory.createTerminal();
        Screen screen = new TerminalScreen(terminal);
        screen.startScreen();
        TextGraphics tg = screen.newTextGraphics();

        // insurance policy for terminal state
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { screen.stopScreen(); } catch (Exception ignored) {}
        }));

        // 1. Create a scheduler to handle the "Tick" and "Draw"
        try (ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor()) {

            scheduler.scheduleAtFixedRate(() -> {
                try {
                    screen.clear();
                    tg.putString(0, 0, "Counter: " + counter.getAndIncrement());
                    tg.putString(0, 2, "Other: " + other.getAndDecrement());
                    screen.refresh();

                    if (other.get() < 0) scheduler.shutdown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, 0, 1, TimeUnit.SECONDS);

            // 2. MAIN INPUT LOOP
            // This thread now does ONLY one thing: check for keys and Ctrl+C
            while (!scheduler.isShutdown()) {
                // This call is now non-blocking and extremely fast
                // It will catch Ctrl+C almost instantly.
                screen.pollInput();

                // We sleep for a very short time just to prevent 100% CPU usage
                Thread.sleep(10);
            }
        }

        screen.stopScreen();
        System.exit(0);
    }
}