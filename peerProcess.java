import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;


public class peerProcess {
	
	//the peerIndex hash map maps the ID to the index 
	//of corresponding record in the neighbors table
	private Config config;
	private FileManager fileManager;
	private MyLogger logger;
	private BitField myBitField;
	private int myID;
	private Record[] neighbors;
	private int numNeighbors;

	public peerProcess(int myID) throws UnknownHostException, IOException {
		
		this.myID = myID;
		this.config = new Config("Common.cfg", "PeerInfo.cfg");
		this.fileManager = new FileManager(this.config, this.myID);
		this.logger = new MyLogger(myID);
		this.myBitField = new BitField(config.getNumPieces());
		this.numNeighbors = config.getNumPeers() - 1;
		neighbors = new Record[numNeighbors];
		
	}
	
	public void initialization(Record record) throws Exception {
		
		Socket socket = record.getUploadSocket();
		HandShakeMsg shake = new HandShakeMsg();
		shake.setID(myID);
		shake.send(socket);
		shake.read(socket);
		
		if (shake.getID() != record.getID()) {
			throw new Exception("Hand shaking fails");
		}
		
		Message bitFieldMsg = new Message();
		//send bit field
		bitFieldMsg.setType(Message.BITFIELD);
		bitFieldMsg.setPayLoad(record.getBitField().toBytes());
		bitFieldMsg.send(socket);
		
		//receive bit field
		bitFieldMsg.read(socket);
		BitField bitField = new BitField(config.getNumPieces());
		bitField.setBitField(bitFieldMsg.getPayLoad());
		record.setBitField(bitField);
		
		Message interest = new Message();
		//send interest or not
		interest.setPayLoad(null);
		if (myBitField.getInterestingIndex(bitField) != -1) {
			interest.setType(Message.INTERESTED);
		} else {
			interest.setType(Message.NOTINTERESTED);
		}
		interest.send(socket);
		//receive interest or not
		interest.read(socket);
		if (interest.getType() == Message.INTERESTED) {
			logger.interestedLog(record.getID());
		} else {
			logger.notInterestedLog(record.getID());
		}
		
		
	}
	
	public void getInitialization(int nextIndex, ServerSocket downloadServSoc, ServerSocket uploadServSoc, 
			ServerSocket haveServSoc) throws IOException {
		
		Socket uploadSocket = uploadServSoc.accept();
		Socket downloadSocket = downloadServSoc.accept(); 
		Socket haveSocket = haveServSoc.accept();
		
		HandShakeMsg shake = new HandShakeMsg();
		shake.read(downloadSocket);
		int ID = shake.getID();
		neighbors[nextIndex] = new Record(config.getNumPieces(), ID, downloadSocket, uploadSocket, haveSocket);
		//send hand shake message
		shake.setID(myID);
		shake.send(downloadSocket);
		
		
		Message bitFieldMsg = new Message();
		
		//receive bit field
		bitFieldMsg.read(downloadSocket);
		BitField bitField = new BitField(config.getNumPieces());
		bitField.setBitField(bitFieldMsg.getPayLoad());
		neighbors[nextIndex].setBitField(bitField);
		
		//send bit field
		bitFieldMsg.setType(Message.BITFIELD);
		bitFieldMsg.setPayLoad(myBitField.toBytes());
		bitFieldMsg.send(downloadSocket);
		
		Message interest = new Message();
		//receive interest or not
		interest.read(downloadSocket);
		if (interest.getType() == Message.INTERESTED) {
			logger.interestedLog(ID);
		} else {
			logger.notInterestedLog(ID);	
		}
		//send interest or not
		interest.setPayLoad(null);
		if (myBitField.getInterestingIndex(neighbors[nextIndex].getBitField()) != -1) {
			interest.setType(Message.INTERESTED);
		} else {
			interest.setType(Message.NOTINTERESTED);
		}
		interest.send(downloadSocket);

		
	}
	
	public void startUpload() throws InterruptedException, ExecutionException, IOException {
		
		ArrayList<Record> sortedInterestingPeers = new ArrayList<Record>();
		ExecutorService uploadThreadPool = Executors.newFixedThreadPool(config.getNumPreferredNeighbors());
		
		while (true) {
			
			sortedInterestingPeers.clear();
			int numFinished = 0;
			
			for (int i = 0; i < numNeighbors; i++) {
				neighbors[i].getState().compareAndSet(1, 0);
				if (neighbors[i].getBitField().isFinished()) {
					numFinished++;
				}
			}
			
			if (numFinished == numNeighbors) {
				Message msg = new Message();
				msg.setType(Message.STOP);
				msg.setPayLoad(null);
				for (int i = 0; i < numNeighbors; i++) {
					msg.send(neighbors[i].getUploadSocket());
				}
				break;
			}

			
			for (int i = 0; i < numNeighbors; i++) {
				
				if ((neighbors[i].getBitField().getInterestingIndex(myBitField) != -1)) {
					sortedInterestingPeers.add(neighbors[i]);
				}
				if (sortedInterestingPeers.size() == 1) {
					continue;
				}
				int j = i - 1;
				int me = i;
				
				while (j >= 0 && sortedInterestingPeers.get(me).getDownload() > sortedInterestingPeers.get(j).getDownload()) {
					
					Record temp = sortedInterestingPeers.get(me);
					sortedInterestingPeers.set(me, sortedInterestingPeers.get(j));
					sortedInterestingPeers.set(j, temp);
					me = j;
					j--;
				}
				
			}
			if (sortedInterestingPeers.size() == 0) {
				Thread.sleep(1000);
				continue;
			}
			
			for (Record r : neighbors) {
				r.clearDownload();
			}
			
			int count = 0;
			ArrayList<Future<Object>> uploadResults = new ArrayList<Future<Object>>();
			ArrayList<Integer> prefN = new ArrayList<Integer>();
			for (Record r : sortedInterestingPeers) {
				count ++;
				if (count <= config.getNumPreferredNeighbors() && r.getState().compareAndSet(0, 1)) {
					Future<Object> f = uploadThreadPool.submit(new Unchoked(r, fileManager, config.getUnChokingInterval(), logger));
					uploadResults.add(f);
					prefN.add(r.getID());
				} 
				
			}
			
			if (prefN.size() > 0) {
				logger.changePrefLog(prefN);
			}
			for (Future<Object> f : uploadResults) {
				f.get();
			}
			
			
			
		}
		
		
	}
	
	public void start() throws Exception {
		
		int myIndex = -1;
		for (int i = 0; i < config.getNumPeers(); i ++) {
			myIndex++;
			if (config.getIDs().get(i) == myID) {
				break;
			}
			
			Socket socket1 = new Socket(config.getAddresses().get(i), config.getUploadPort(i));		
			Socket socket2 = new Socket(config.getAddresses().get(i), config.getDownloadPort(i));
			Socket socket3 = new Socket(config.getAddresses().get(i), config.getHavePort(i));
			logger.TCPConnToLog(config.getIDs().get(i));
			
			neighbors[i] = new Record(config.getNumPieces(),  config.getIDs().get(i), socket1, socket2, socket3);
			initialization(neighbors[i]);
		}
		
		ServerSocket downloadServSoc = new ServerSocket(config.getDownloadPort(myIndex));
		ServerSocket uploadServSoc = new ServerSocket(config.getUploadPort(myIndex));
		ServerSocket haveServSoc = new ServerSocket(config.getHavePort(myIndex));
		
		for (int i = myIndex; i < config.getNumPeers() - 1; i++) {
			getInitialization(i, downloadServSoc, uploadServSoc, haveServSoc);
		}
		
		
		ExecutorService downloadThreadPool = Executors.newFixedThreadPool(numNeighbors);
		ArrayList<Future<Object>> downloadResults = new ArrayList<Future<Object>>();
		
		//start opt unchoking upload
		ExecutorService OptUpload = Executors.newSingleThreadExecutor();
		Future<Object> OptUploadResult = OptUpload.submit(new OptUnchoked(myBitField, neighbors, fileManager, config.getOptUnChokingInterval(), logger));
		
		ExecutorService haveThreadPool = Executors.newFixedThreadPool(numNeighbors);
		ArrayList<Future<Object>> haveResults = new ArrayList<Future<Object>>();
		
		//start download and have listening
		for (int i = 0; i < numNeighbors; i++) {
			Record r = neighbors[i];
			Future<Object> f1 =  downloadThreadPool.submit(new Download(r, neighbors, myBitField, fileManager, logger));
			downloadResults.add(f1);
			Future<Object> f2 = haveThreadPool.submit(new HaveListener(r, logger));
			haveResults.add(f2);
		}
				
		//start upload
		//blocking method
		//return when upload finishes
		startUpload();
		//waiting for all download stop
		for (int i = 0; i < numNeighbors; i++) {
			
			downloadResults.get(i).get();
			haveResults.get(i).get();
			
		}
		OptUploadResult.get();
		
		downloadServSoc.close();
		uploadServSoc.close();
		haveServSoc.close();
		for (int i = 0; i < numNeighbors; i++) {
			neighbors[i].getDownloadSocket().close();
			neighbors[i].getUploadSocket().close();
			neighbors[i].getHaveSocket().close();
		}
	}
	
	public static void main(String args[]) throws Exception {
		
		peerProcess peer = new peerProcess(Integer.parseInt(args[0]));
		peer.start();
	}
}