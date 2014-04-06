import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.Callable;

public class OptUnchoked implements Callable<Object>{ 
	
	private BitField myBitField;
	private Record[] neighbors;
	private FileManager fileManager;
	private int time;
	private MyLogger logger;
	private static final int TIMEOFTRY = 10;

	
	public OptUnchoked(BitField myBitField, Record[] neighbors, FileManager fileManager, int time, MyLogger logger ) {
		
		this.myBitField = myBitField;
		this.neighbors = neighbors;
		this.fileManager = fileManager;
        this.time=time;
        this.logger = logger;
	}
	
	public Object call() throws IOException, InterruptedException {
		
		while (true) {
			
			boolean allFinished = true;
			for (int i = 0; i < neighbors.length; i++) {
				if (!neighbors[i].getBitField().isFinished()) {
					allFinished = false;
					break;
				}
			}
			
			if (allFinished) {
				break;
			}
			int optIndex = -1;
			boolean find = false;
			for (int i = 0; i < TIMEOFTRY; i++) {
				optIndex=(int)(Math.random()*neighbors.length);
				if ((neighbors[optIndex].getBitField().getInterestingIndex(myBitField) != -1) && neighbors[optIndex].getState().compareAndSet(0, 2)) {
					find = true; 
					break;
				}
			}
			if (!find) {
				Thread.sleep(1000);
				continue;
			}
			
			
			logger.changeOptLog(optIndex);
			Record record = neighbors[optIndex];
			Socket socket = record.getUploadSocket();
			Message msg = new Message();
			msg.setType(Message.UNCHOKE);
			msg.setPayLoad(null);
			msg.send(socket);
			
			long start = System.currentTimeMillis();
			
			while (true) {
				
				msg.read(socket);
				if (msg.getType() == Message.NOTINTERESTED) {
					logger.notInterestedLog(record.getID());
					break;
				}
				
				if (msg.getType() == Message.REQUEST) {
					int index = Conversion.BytesToInt(msg.getPayLoad());
					Piece uploadPiece = fileManager.readPiece(index);
					msg.setType(Message.PIECE);
					msg.setPayLoad(uploadPiece.getPieceBytes());
					msg.send(socket);
				}
				
				//check if timeout
				if ((System.currentTimeMillis() - start) > time * 1000) {
					msg.setType(Message.CHOKE);
					msg.setPayLoad(null);
					msg.send(socket);
					break;
				} 
			}
			
			neighbors[optIndex].getState().compareAndSet(2, 0);
	
 		}
		return new Object();
		}

}
