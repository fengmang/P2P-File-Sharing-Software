import java.io.IOException;
import java.net.*;
import java.util.concurrent.*;

public class Download implements Callable<Object> {
	
	private Record record;
	private BitField myBitField;
	private Socket socket;
	private FileManager fileManager;
	private Record[] neighbors;
	private MyLogger logger;
	
	public Download(Record record, Record[] neighbors, BitField myBitField, FileManager fileManager, MyLogger logger) {
		
		this.neighbors = neighbors;
		this.record = record;
		this.myBitField = myBitField;
		this.socket = record.getDownloadSocket();
		this.fileManager = fileManager;
		this.logger = logger;
		
	}
	
	public Object call() throws IOException {
		
		Message msg = new Message();
		//jump out the while loop only when receiving STOP message
		while (true) {
			msg.read(socket);
			
			if (msg.getType() == Message.STOP) {
				for (int i = 0; i < neighbors.length; i++) {
					msg.send(neighbors[i].getHaveSocket());
				}
				break;
			}
			
			else if (msg.getType() == Message.UNCHOKE) {
				//jump out the while loop only when receiving CHOKE message or not interests to the sender
				logger.unchokingLog(record.getID());
				while (true) {
					int interestedPiece = myBitField.getInterestingIndex(record.getBitField());
					if (interestedPiece == -1) {
						msg.setType(Message.NOTINTERESTED);
						msg.setPayLoad(null);
						msg.send(socket);
						break;
					} else {
						//send REQUEST message
						msg.setType(Message.REQUEST);
						msg.setPayLoad(Conversion.IntToBytes(interestedPiece));
						msg.send(socket);
						
						//download a piece
						msg.read(socket);
						Piece downloadPiece = new Piece(interestedPiece, msg.getPayLoad());
						fileManager.writePiece(downloadPiece);
						myBitField.turnOnBit(interestedPiece);
						record.incDownload();
						logger.downloadLog(record.getID(), interestedPiece);
						if (myBitField.isFinished()) {
							logger.compDownloadLog();
						}
								
						//send HAVE message
						msg.setType(Message.HAVE);
						msg.setPayLoad(Conversion.IntToBytes(interestedPiece));
						for (int i = 0; i < neighbors.length; i++) {
							msg.send(neighbors[i].getHaveSocket());
						}
					}
				}
			} else if (msg.getType() == Message.CHOKE) {
				logger.choking(record.getID());
				break;
			}
			
		}
		
		return new Object();
	}
}
