import java.io.IOException;
import java.net.*;
import java.util.concurrent.*;
//stop listening only when receive STOP message
public class HaveListener implements Callable<Object>{
	
	private Record record;
	private MyLogger logger;
	
	public HaveListener(Record record, MyLogger logger) {
		this.record = record;
		this.logger = logger;
	}
	
	public Object call() throws IOException {
		
		Socket socket = record.getHaveSocket();
		Message Msg = new Message();
		
		while (true) {
			
			Msg.read(socket);
			if (Msg.getType() == Message.STOP) {
				break;
			} else {
				if (Msg.getType() == Message.HAVE) {
					
					byte[] payLoad = Msg.getPayLoad();
					int index = Conversion.BytesToInt(payLoad);
					record.getBitField().turnOnBit(index);
					logger.haveLog(record.getID(), index);
				}
			}

		}
		
		return new Object();
	}
	
	
}
