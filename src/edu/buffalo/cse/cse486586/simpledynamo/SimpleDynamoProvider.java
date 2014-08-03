package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.StringTokenizer;
import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	Context rcontext;
	ContentResolver conRes;
	Uri mUri = buildUri("content","edu.buffalo.cse.cse486586.simpledynamo.provider");
	static String emulNum;
	static String successor;
	static String successor2;
	static String predecessor;
	static String predecessor2;
	static Boolean smallestFlag=false;
	static String queryValue;
	static int gcount = 0;
	//static String up;
	static int dcount = 0;
	String Port[]={"5562","5556","5554","5558","5560"};
	static HashMap<String, String> succList;
	static HashMap<String, String> predList;

	private static final String KEY_S = "key";
	private static final String VALUE_S = "value";
	static String[] cols = { KEY_S, VALUE_S };
	static MatrixCursor globalCursor = new MatrixCursor(cols);

	@Override
	public boolean onCreate() {
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		rcontext = getContext();
		emulNum = portStr;
		conRes = rcontext.getContentResolver();

		succList = new HashMap<String, String>();
		succList.put("5562", "5556");succList.put("5556", "5554");succList.put("5554", "5558");succList.put("5558", "5560");succList.put("5560", "5562");

		if (emulNum.equals("5560"))
			smallestFlag = true;

		successor = succList.get(emulNum);
		successor2 = succList.get(successor);

		predList = new HashMap<String, String>();
		predList.put("5560", "5558");predList.put("5558", "5554");predList.put("5554", "5556");predList.put("5556", "5562");predList.put("5562", "5560");

		predecessor = predList.get(emulNum);
		predecessor2=predList.get(predecessor);
		// initiate the server thread
		//Log.i("log","Oncreate of "+emulNum+"succ1"+successor+"succ2:"+successor2+"pred:"+predecessor);
		Thread serv = new Server();
		serv.start();
		getRecovery();
		return false;
	}

	public void getRecovery(){

		String[] savFiles = rcontext.fileList();
		//Log.i("log","delete: count of files in node before delete:"+savFiles.length);
		for (int i = 0; i < savFiles.length; i++)
			rcontext.deleteFile(savFiles[i]);
		//Log.i("log","delete: count of files in node after delete:"+savFiles.length);
		for(int i=0;i<5;i++){
			if(!Port[i].equals(emulNum)){
				//Log.i("log","getRecovery:send recovery request from:"+emulNum+",to:"+Port[i]);
				//attaches R and queries @ on all the avd to collect its all keys
				new Thread(new Client(emulNum,Integer.parseInt(Port[i]) * 2, 1)).start();
			}
		}
	}

	//delete function for deleting selection= * , @ and key
	@Override
	public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
		dcount=0;
		String key=selection;
		if (selection != null && selection.length() == 1) {
			if (selection.equals("@")||selection.equals("*")) {
				String[] savFiles = rcontext.fileList();
				for (int i = 0; i < savFiles.length; i++)
					rcontext.deleteFile(savFiles[i]);
			}
			if (selection.equals("*")) {
				new Thread(new Client(emulNum,Integer.parseInt(successor) * 2, 10)).start();
				while(dcount==1){}
			}
		}       
		else if(selection != null && selection.length() >1){
			//Log.i("log","delete:of:"+emulNum+",deleteAt:"+deleteAt+",key:"+key.substring(0,5));
			try {  				
				rcontext.deleteFile(key);//it queries the key on its on dump and deletes it
			} 
			catch (Exception e) {
				Log.i("MSG","delete:Error in delete"+e);
			}
			for(int i=0;i<5;i++){
				if(!Port[i].equals(emulNum)){
					//Log.i("log","sending delete from:"+emulNum+",to:"+deleteAt+",key:"+key.substring(0,5));
					new Thread(new Client(key + ":" + emulNum,Integer.parseInt(Port[i]) * 2, 11)).start();//attaches #
				}
			}
		}
		return 0;
	}

	public void actualInsert(String key, String value) {
		try {
			//Log.i("log","actualinsert::of:"+emulNum+"for key:"+key.substring(0,5)+",value:"+value.substring(0,5));
			FileOutputStream fos = rcontext.openFileOutput(key,Context.MODE_PRIVATE);
			OutputStreamWriter osw = new OutputStreamWriter(fos);
			osw.write(value);
			osw.flush();
			osw.close();
		} catch (Exception e) {
			Log.i("log", "Exception e = " + e);
		}
	}

	public String lookUp(String key){
		String lookAt=null;
		try {
			if(genHash(key).compareTo(genHash("5562"))>0 && genHash(key).compareTo(genHash("5556"))<=0)lookAt="5556";
			else if(genHash(key).compareTo(genHash("5556"))>0 && genHash(key).compareTo(genHash("5554"))<=0) lookAt="5554";
			else if(genHash(key).compareTo(genHash("5554"))>0 && genHash(key).compareTo(genHash("5558"))<=0) lookAt="5558";
			else if(genHash(key).compareTo(genHash("5558"))>0 && genHash(key).compareTo(genHash("5560"))<=0) lookAt="5560";
			else	lookAt="5562";
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return lookAt;
	}

	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {   
		String key = (String) values.get(KEY_S);
		String value = (String) values.get(VALUE_S);
		String insertAt=null;
		insertAt=lookUp(key);
		//Log.i("log","insert:of:"+emulNum+",insertAt:"+insertAt+",key:"+key.substring(0,5)+",value:"+value.substring(0,5));
		//transfers the key to emul where actual insertion is to be done
		if(insertAt.equals(emulNum))
			actualInsert(key,value);
		else
			new Thread(new Client(key + ":" + value,Integer.parseInt(insertAt) * 2, 6)).start();//attaches I and calls actualInsert
		//Log.i("log","insert:of:"+emulNum+",insertAt:"+succList.get(insertAt)+",key:"+key.substring(0,5)+",value:"+value.substring(0,5));
		new Thread(new Client(key + ":" + value,Integer.parseInt(succList.get(insertAt)) * 2, 6)).start();//attaches I and calls actualInsert
		//Log.i("log","insert:of:"+emulNum+",insertAt:"+succList.get(succList.get(insertAt))+",key:"+key.substring(0,5)+",value:"+value.substring(0,5));
		new Thread(new Client(key + ":" + value,Integer.parseInt(succList.get(succList.get(insertAt)))* 2, 6)).start();//attaches I and calls actualInsert
		getContext().getContentResolver().notifyChange(uri, null);
		return uri;
	}

	String[] actualQuery(String key) {
		//Log.i("log","in actualQuery for key:"+key);
		String fname = key;
		String value;
		try {
			FileInputStream fin = rcontext.openFileInput(fname);
			InputStreamReader inpReader = new InputStreamReader(fin);
			BufferedReader br = new BufferedReader(inpReader);
			value = br.readLine();
		} catch (Exception e) {
			value = null;
			Log.i("log","value is null");
		}
		//Log.i("log","in actualQuery for key:"+key+"value returned:"+value);
		String[] row = { key, value };
		return row;
	}

	public MatrixCursor getlocalDump(){
		MatrixCursor tempCursor = new MatrixCursor(cols);
		String[] savFiles = rcontext.fileList();
		for (int i = 0; i < savFiles.length; i++) {
			tempCursor.addRow(actualQuery(savFiles[i]));
		}
		return tempCursor;
	}
	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,String sortOrder) {  
		//Log.i("log", "in query");
		MatrixCursor localCursor = new MatrixCursor(cols);
		queryValue = null;
		gcount=0;
		String key=selection;
		if (selection != null && selection.length() == 1) {
			if (selection.equals("@")) 
				localCursor=getlocalDump();
			if (selection.equals("*")) {
				//Log.i("log", "query * at:"+emulNum+":fwd to"+successor);
				new Thread(new Client(emulNum,Integer.parseInt(successor) * 2, 8)).start();//attaches (* + localdump) by calling getldump2,then calls getldump
				while (gcount ==0) {}
				//Log.i("log","updating global cursor");
				return globalCursor;
			}
		} 
		else {

			String queryAt=lookUp(key);//tells where the key shud lie
			//Log.i("log", "query:"+emulNum+":queryAt:"+queryAt+"for key:"+key.substring(0,5)+",present qv:"+queryValue);
			if(emulNum.equals(queryAt)){
				//Log.i("query","got answer of query at local dump");
				String[] row=actualQuery(key);
				queryValue=row[1];
				localCursor.addRow(row);
			}
			else if(queryValue==null){
				//Log.i("log", "query:for key:"+key.substring(0,5)+"queryValue still null sending query to:"+queryAt);
				//check if queryAt is up or not, if up returns value in var->queryValue
				new Thread(new Client(key + ":" + emulNum+"="+queryAt, Integer.parseInt(queryAt) * 2, 7)).start();//attaches Q
				
				//Log.i("log", "query:for key:"+key.substring(0,5)+"after waking up:"+emulNum+",response frm:"+queryAt+",got queryValue:"+queryValue);
				
				while(queryValue==null){}
			//	Log.i("log", "query:"+emulNum+":queryAt:"+queryAt+"for key:"+key.substring(0,5)+",value returned:"+queryValue.substring(0,5));
				String[] row = {key, queryValue};
				localCursor.addRow(row);
			}
		}
		return localCursor;
	}

	//used to get all the key,value pairs from whole dht and put in gcursor
	void getldump(String msg) {//msg is of form originport:ldump from originport
		String[] emptyCur={"",""};   
		StringTokenizer sTok = new StringTokenizer(msg, ":");
		String originPort = sTok.nextToken();
		if(originPort.equals(emulNum)){
			//termination condition for query as ldump contains port numbr only when all files are deleted
			if(msg.length()>4){
				String gDump=msg.substring(5);
				String[] strSplit = gDump.split(":");
				for(int i =0; i < strSplit.length ; i++) {
					String temp = strSplit[i];
					String[] strSplit2 = temp.split(",");
					String key = strSplit2[0];
					String value = strSplit2[1];
					String[] row = { key, value };
					globalCursor.addRow(row);
				}
			}
			else
				globalCursor.addRow(emptyCur);
			gcount=1;           
		}
		else{
			//Log.i("log", "in query req *:at "+emulNum+",fwd to:"+successor);
			new Thread(new Client(msg, Integer.parseInt(successor) * 2, 8)).start();//attaches (* + localdump) by calling getldump2,then calls getldump

		}
	}

	//used to get local dump from an emulator and return the whole list as a string of key1,val1:key2,val2...
	String getldump2() {
		String ldump = "";
		Cursor resultCursor = getlocalDump();
		int keyIndex = resultCursor.getColumnIndex("key");
		int valueIndex = resultCursor.getColumnIndex("value");
		resultCursor.moveToFirst();
		while (!resultCursor.isAfterLast()) {
			String key = resultCursor.getString(keyIndex);
			String val = resultCursor.getString(valueIndex);
			String kvPair = ":"+key + "," + val;
			ldump = ldump + kvPair;
			resultCursor.moveToNext();
		}
		resultCursor.close();
		return ldump;
	}

	public class Client implements Runnable {
		String msg="";
		int type, port;

		Client(String msg, int port, int msgType) {
			this.msg = msg;
			this.port = port;
			type = msgType;
		}
		@Override
		public void run() {
			//Log.i("log", "in client port:"+emulNum +",msg is:"+msg);
			Socket clSock;
			try {
				clSock = new Socket("10.0.2.2", port);// connect to server
				//clSock.setSoTimeout(2000);
				PrintWriter sendData = new PrintWriter(clSock.getOutputStream());// send the message to server
				BufferedReader readData = new BufferedReader(new InputStreamReader(clSock.getInputStream()));
				if (type == 1){// recovery
					//Log.i("log", "client with msgType:R-,to port:"+emulNum);
					sendData.println("R" + msg);//msg=originport
				}
				else if (type == 2) {// recovery response
					//Log.i("log", "client with msgType:r,to port:"+emulNum);
					sendData.println("r" + msg);
				}
				else if (type == 5){ // query response
					//Log.i("log", "client with type 5,in port:"+emulNum+"sending msg:^:" + msg);
					sendData.println("^" + msg);
				}
				else if (type == 6) // insert
					sendData.println("I" + msg);
				else if (type == 7){ // query
					//Log.i("log", "client with type 7 sending msg:"+msg+",in port:"+emulNum);
					String[] split=msg.split("=");
					String queryAt=split[1];
					//Log.i("log", "client:queryAt is:"+split[1]);
					sendData.println("Q" + split[0]);//Log.i("log", "client:sending msg:Q" + split[0]);
					sendData.flush();
					queryValue=readData.readLine();
					
					//Log.i("log", "client with ack:"+queryValue);
					if (queryValue==null){
						//Log.i("log", "client: passing query to:"+succList.get(queryAt)+",msg:"+split[0]+"="+succList.get(queryAt));
						new Thread(new Client(split[0]+"="+succList.get(queryAt), Integer.parseInt(succList.get(queryAt)) * 2, 7)).start();
					}
				}
				else if (type == 8){// gdump
					//Log.i("log", "client BEFORE WRITING");
					sendData.println("*" + msg+getldump2());
					sendData.flush();
					String ack=readData.readLine();
					//Log.i("log", "client with ack:"+ack);
					if (ack==null){
						//Log.i("log", "client passing to succ2");
						new Thread(new Client(msg, Integer.parseInt(successor2) * 2, 8)).start();
					}
				}
				else if (type == 9)// gdump reply
					sendData.println("(" + msg);
				else if (type == 10)// delete
					sendData.println("-" + msg);//msg=originport
				else if (type == 11)// delete key
					sendData.println("#" + msg);//msg=originport
				sendData.flush();
				sendData.close();
				clSock.close();
			} catch (NumberFormatException e) {
				Log.i("log", "Client Exception: Number format Exception!\n");
				e.printStackTrace();
			} catch (UnknownHostException e) {
				Log.i("log", "Client Exception: UnknownHostException!\n");
				e.printStackTrace();
			} catch (IOException e) {
				Log.i("log", "Client Exception: I/O error occured when creating the socket!\n");
				e.printStackTrace();
			} catch (SecurityException e) {
				Log.i("log", "Client Exception: SecurityException!\n");
				e.printStackTrace();
			}
		}
	}

	class Server extends Thread {

		public void run() {
			try {
				ServerSocket serSock = new ServerSocket(10000);
				//PrintWriter sendData = new PrintWriter(clSock.getOutputStream());
				//Log.i("log","server:"+emulNum);
				while (true) {
					Socket recvSock = serSock.accept();// listen for client
					InputStreamReader readStream = new InputStreamReader(recvSock.getInputStream());// get the message
					BufferedReader recvInp = new BufferedReader(readStream);

					PrintWriter sendData = new PrintWriter(recvSock.getOutputStream());// send the message to server
					String recvMsg = recvInp.readLine();
					char msgType=recvMsg.charAt(0);// recognise message type
					// insert
					if(msgType=='I'){
						String passKeyVal=recvMsg.substring(1);
						String[] strSplit = passKeyVal.split(":");
						String key = strSplit[0];
						String value = strSplit[1];
						actualInsert(key, value);
					}// query type 7
					else if(msgType=='Q'){ 
						//Log.i("log","Server: in msg type:Q");
						String msg=recvMsg.substring(1);
						String[] strSplit = msg.split(":");
						String key = strSplit[0];
						String orginPort = strSplit[1];
						String[] result = actualQuery(key);//it queries the key on its on dump
						String value = result[1];
						//Log.i("log","Server of:"+emulNum+" in msg type:Q value is:"+value+",returning value to:"+orginPort);
						//attaches ^ and the value and sends back to origin query
						//new Thread(new Client(value,Integer.parseInt(orginPort) * 2, 5)).start();
						
						sendData.println(value);
						sendData.flush();
						
						//Log.i("log","Server of:"+emulNum+" in msg type:Q value is:"+value+",returned value to:"+orginPort);
					}//query response type 5
					else if(msgType=='^'){
						//Log.i("log","Server: in msg type:^:queryvalue before modifying is:"+queryValue+"at:"+emulNum);
						if(queryValue==null){
							queryValue = recvMsg.substring(1);
							//Log.i("log","Server: in msg type:^:queryvalue after modifying is:"+queryValue+"at:"+emulNum);
						}
						//Log.i("log","Server: in msg type:^:queryvalue after if is:"+queryValue+"at:"+emulNum);
					}
					else if(msgType=='*'){//gdump
						//Log.i("log","server: IN *");
						sendData.println("ACK\n");
						sendData.flush();
						//Log.i("log","ack sent");

						getldump(recvMsg.substring(1));
					}
					else if(msgType=='-'){//delete all dht ENTRY
						String originPort=recvMsg.substring(1);
						String sel = "@";
						if(originPort==emulNum){
							dcount=1;
						}else{
							conRes.delete(mUri,sel,null);
							new Thread(new Client(originPort,Integer.parseInt(successor) * 2, 10)).start();
						}       
					}//delete a particular key
					else if(msgType=='#'){
						String[] strSplit = recvMsg.substring(1).split(":");
						String key = strSplit[0];
						//Log.i("log","server: file at:"+emulNum+",fname:"+key);
						//File location=new File(getContext().getFilesDir().getAbsolutePath());
						rcontext.deleteFile(key);//it queries the key on its on dump and deletes it
					}
					//recovery type 1
					else if(msgType=='R'){ 
						//Log.i("log","server:msgtype:"+msgType);
						String originPort=recvMsg.substring(1);//refers to the recovery node
						Cursor resultCursor = getlocalDump();
						String ldump=emulNum;
						if(resultCursor.getCount()>0){
							//Log.i("log", "Server of:"+emulNum+",numbr of rows in cursor"+resultCursor.getCount());
							int keyIndex = resultCursor.getColumnIndex("key");
							int valueIndex = resultCursor.getColumnIndex("value");
							resultCursor.moveToFirst();
							while (!resultCursor.isAfterLast()) {
								String key = resultCursor.getString(keyIndex);
								String val = resultCursor.getString(valueIndex);
								String kvPair = ":"+key + "," + val;
								ldump = ldump + kvPair;
								resultCursor.moveToNext();
							}
							resultCursor.close();
							//ldump is of form originport:k,v:k,v....

							//Log.i("log", "Server:sent ldump:"+ldump.substring(0,70)+", to:"+originPort);
							new Thread(new Client(ldump,Integer.parseInt(originPort) * 2, 2)).start();//returns own local dump to avd who wants to recover
						}//Log.i("log", "Server:recovery response sent");
					}//recovery response type 2
					else if(msgType=='r'){
						//synchronized(this){
						//Log.i("log", "Server:in msgtype:"+msgType);
						String recResponse=recvMsg.substring(1);
						//Log.i("log", "Server:response of recovery from:"+recResponse.substring(0,70));
						collectionRecovery(recResponse);
						//	}
					}
					sendData.close();
					recvSock.close();
				}
			} catch (IOException e) {
				//Log.i("MSG", "Server: I/O error occured when creating the socket!\n");
				e.printStackTrace();
			}
		}
	}
	//recovery function
	public void collectionRecovery(String recv){
		int count=0;
		String storeAt=null;
		String recvPort=recv.substring(0,4);//get backs to origin port after collecting dump
		if(recv.length()>4){
			//Log.i("log", "collectionRecovery:recoveryPort:"+recvPort);
			String kvResponse=recv.substring(5);
			if(kvResponse.length()>4){
				String[] strSplit = kvResponse.split(":");
				//Log.i("log","collectionRecovery:"+strSplit.length+" keys collected from:"+recvPort+",kv starting:"+kvResponse.substring(0,5));
				for(int i =0; i < strSplit.length ; i++) {
					count++;
					String temp = strSplit[i];//Log.i("log","collectionRecovery:kv is:"+temp);
					String[] strSplit2 = temp.split(",");
					String key = strSplit2[0];
					String value = strSplit2[1];
					storeAt=lookUp(key);
					if(storeAt.equals(emulNum)||storeAt.equals(predecessor)||storeAt.equals(predecessor2)){
						//Log.i("log","collectionRecovery: "+emulNum+": key coordinator is: "+storeAt);
						Log.i("log","collectionRecovery:inserts: key:"+key+",val:"+value);
						actualInsert(key, value);
					}
				}
				Log.i("log","collectionRecovery:"+strSplit.length+" keys recovered from:"+recvPort+"loop runned:"+count);
			}
			else
				Log.i("log","collectionRecovery:msg len smaller then 4: it is"+kvResponse.length());
		}
	}
	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		return 0;
	}

	// build Uri
	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	@Override
	public String getType(Uri uri) {
		return null;
	}
	//function for calculating genHash
	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}
}