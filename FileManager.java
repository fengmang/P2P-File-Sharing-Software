import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;


public class FileManager {

	private Config myConfig;
	private RandomAccessFile file;

	public FileManager(Config myConfig ,int peerID) throws FileNotFoundException {
		
		this.myConfig = myConfig;
		String directory =  "peer_" + peerID + "/";
		File dir = new File(directory);
		if (!dir.exists()) {
			dir.mkdirs();
		file = new RandomAccessFile(directory + myConfig.getFileName(), "rw");
		
		}
		
	}

	//read a piece from the file, which starts from 0;
	public Piece readPiece(int which) throws IOException {
		int length = 0;
		if (which == myConfig.getNumPieces() - 1) {
			length = myConfig.getFileSize() - myConfig.getPieceSize() * which;
		} else {
			length = myConfig.getPieceSize();
		}
		int offSet = which*myConfig.getPieceSize();
		byte[] bytes = new byte[length];
		file.readFully(bytes, offSet, length);
		Piece piece = new Piece(which, bytes);
		return piece;
		
		}
    //store the piece
	
	public void writePiece(Piece piece) throws IOException {
		
		int offSet = piece.getWhichPiece()*myConfig.getPieceSize();
		int length = piece.getPieceBytes().length;
		file.write(piece.getPieceBytes(), offSet, length);
		}
	
	}

