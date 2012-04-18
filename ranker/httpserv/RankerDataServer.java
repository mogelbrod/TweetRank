import java.io.IOException;
import java.net.InetSocketAddress;

import com.sun.net.httpserver.HttpServer;


public class RankerDataServer {
    public static final int PORT = 4711;
    private HttpServer server;
    private InetSocketAddress addr;
    private int backlog;
	
    public RankerDataServer(InetSocketAddress addr, int backlog) throws IOException {
	super();
	this.addr = addr;
	this.backlog = backlog;

	server = HttpServer.create(addr, backlog);
	server.createContext("/", new RequestHandler());
	server.setExecutor(null);
    }

    public void start() {
	server.start();
    }
	
    public static void main(String[] args) {
	try {
	    RankerDataServer server = new RankerDataServer(new InetSocketAddress(PORT), 0);
	    server.start();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }
}
