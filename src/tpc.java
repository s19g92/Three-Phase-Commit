/*
 * @author Shubham Gupta
 */

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

@SuppressWarnings({ "rawtypes", "unused" })
class tpc {

	// Common Variables and Data Storage.
	static String coordinator = "";
	static String hostname = "";
	static int no_process;
	static int t_id;
	static int value;
	static boolean aborting = false;
	static boolean commit = false;
	static String stateFile = "";

	// Coordinator only data.
	static int active_process = 0;
	static int ready_process = 0;
	static int agreed_count = 0;
	static int ack_count = 0;
	static boolean ack;
	static boolean agreed;
	static boolean is_coord = false;
	static Map<String, String> id_hostnames = new HashMap<String, String>();

	// Individual Process Variables.
	static String id = "";
	static boolean prepare = false;
	static boolean reg_complete = false;
	static Socket socket;

	/****************************** MAIN FUNCTION *********************************************/

	public static void main(String[] args) {

		if (args.length != 0) {
			if (args[0].equalsIgnoreCase("-c")) {
				is_coord = true;
				id = "0";
				stateFile = "state_" + id + ".txt";

				Scanner sc3 = null;
				File f = new File("commit_" + id + ".txt");
				if (f.exists() && !f.isDirectory()) {
					try {
						sc3 = new Scanner(new File("commit_" + id + ".txt"));
					} catch (FileNotFoundException e) {
					}
					while (sc3.hasNextLine()) {
						String next = sc3.nextLine();
						String[] list;
						if (!next.isEmpty()) {
							list = next.split("\\s");
							if (list[0].equalsIgnoreCase("commit")) {
								t_id = Integer.valueOf(list[1]) + 1;
							}
						}
					}
				}
			}

		/*	else if (args[0].equalsIgnoreCase("-r")) {
				Scanner sc3 = null;
				File f = new File("commit_" + id + ".txt");
				if (f.exists() && !f.isDirectory()) {
					try {
						sc3 = new Scanner(new File("commit_" + id + ".txt"));
					} catch (FileNotFoundException e) {
					}
					while (sc3.hasNextLine()) {
						String next = sc3.nextLine();
						String[] list;
						if (!next.isEmpty()) {
							list = next.split("\\s");
							if (list[0].equalsIgnoreCase("commit")) {
								t_id = Integer.valueOf(list[1]) + 1;
							}
						}
					}
				}
			}*/
		}
		read_config();

		System.out.println("Coordinator : " + coordinator);
		System.out.println("Is coordinator : " + is_coord);

		// If the node is the coordinator then start listener.
		Thread start = new Thread() {
			public void run() {
				if (is_coord) {
					coord_register_new();
					recv_msg();
					// If the node is not the coordinator send register.
				} else {
					process_register();
					recv_msg();
				}
				System.out.println("Program thread exited");
			}
		};
		start.start();
	}

	// Read from the dsConfig file the stats.
	public static void read_config() {
		// Read the dsConfig File and assign the data to proper variables.
		Scanner sc2 = null;
		String[] list = null;
		try {
			sc2 = new Scanner(new File("dsConfig"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		while (sc2.hasNextLine()) {
			String next = sc2.nextLine();
			if (!next.isEmpty()) {
				list = next.split("\\s");
				if (list[0].equalsIgnoreCase("coordinator")) {
					coordinator = list[1];
				} else if (list[0].equalsIgnoreCase("number")) {
					no_process = Integer.valueOf(list[3]);
				}
			}
		}
	}

	// Send a message to another process.
	public static void send_msg(String host, int port, String sendMessage) {
		try {

			InetAddress address = InetAddress.getByName(host);
			socket = new Socket(address, port);

			// Send the message to the host
			OutputStream os = socket.getOutputStream();
			OutputStreamWriter osw = new OutputStreamWriter(os);
			BufferedWriter bw = new BufferedWriter(osw);

			bw.write(sendMessage + "!");
			bw.flush();

		} catch (Exception exception) {
			exception.printStackTrace();
		} finally {
			// Closing the socket
			try {
				// socket.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	// A message is received through a channel from another process.
	@SuppressWarnings("resource")
	public static void recv_msg() {

		String message = "";
		try {

			int port = 25555;
			ServerSocket serverSocket = new ServerSocket(port);
			System.out.println("Process listening to the port 25555");

			// Process is listening for new messages after registeration.
			while (true) {

				// Reading the message from the other process.
				socket = serverSocket.accept();

				InputStream is = socket.getInputStream();
				String hostName = socket.getInetAddress().getHostName();
				InputStreamReader isr = new InputStreamReader(is);
				BufferedReader br = new BufferedReader(isr);
				final char endMarker = '!';

				// StringBuffer if you need to be threadsafe
				StringBuilder messageBuffer = new StringBuilder();
				int value;
				// reads to the end of the stream or till end of message
				while ((value = br.read()) != -1) {
					char c = (char) value;
					if (c == endMarker) {
						break;
					} else {
						messageBuffer.append(c);
					}
				}
				// message is complete!
				message = messageBuffer.toString();

				if (message != null && message != "") {
					System.out.println("Message received from " + hostName
							+ " is " + message);
					final String msg = message;
					Thread one = new Thread() {
						public void run() {
							try {
								String[] list = msg.split(" ");
								msg_type(list);
							} catch (NumberFormatException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					};
					one.start();
				}
			}
		} catch (Exception exception) {
			exception.printStackTrace();
		} finally {
			// Closing the socket
			try {
				// socket.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	// Send the inital register message to the coordinator
	// if the process itself is not the coordinator.
	public static void process_register() {

		if (!is_coord) {
			String msg = "REGISTER";
			int port = 25555;
			String host = coordinator + ".utdallas.edu";
			try {

				InetAddress address = InetAddress.getByName(host);
				socket = new Socket(address, port);

				// Send the message to the host
				OutputStream os = socket.getOutputStream();
				OutputStreamWriter osw = new OutputStreamWriter(os);
				BufferedWriter bw = new BufferedWriter(osw);

				System.out.println("Message " + msg + " sent to the : " + host);
				bw.write(msg + "!");
				bw.flush();

				// Get the return message from the coordinator.
				InputStream is = socket.getInputStream();
				InputStreamReader isr = new InputStreamReader(is);
				BufferedReader br = new BufferedReader(isr);

				final char endMarker = '!';

				// StringBuffer if you need to be threadsafe
				StringBuilder messageBuffer = new StringBuilder();
				int value;
				// reads to the end of the stream or till end of message
				while ((value = br.read()) != -1) {
					char c = (char) value;
					if (c == endMarker) {
						break;
					} else {
						messageBuffer.append(c);
					}
				}
				// message is complete!
				String message = messageBuffer.toString();

				if (message != null) {
					System.out.println("Message received : " + message);
					String[] msg_list = message.split(" ");
					if (msg_list[0].equalsIgnoreCase("ack")) {

						// Check the id assigned and store the corresponding
						// values.
						id = msg_list[1];
						stateFile = "state_" + id + ".txt";
						System.out.println("Id received : " + id);
					}
				}

			} catch (Exception exception) {
				exception.printStackTrace();
			} finally {
				// Closing the socket
				try {
					// socket.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	// Processing the register requests received from
	// non-coordinator processes.
	public static void coord_register_new() {

		try {

			int port = 25555;
			@SuppressWarnings("resource")
			ServerSocket serverSocket = new ServerSocket(port);
			System.out.println("Coordinator listening to the port 25555");

			// Coordinator is listening always for new process registering.
			while (true) {

				// Reading the message from the other process.
				socket = serverSocket.accept();

				String hostName = socket.getInetAddress().getHostName();
				InputStream is = socket.getInputStream();
				InputStreamReader isr = new InputStreamReader(is);
				BufferedReader br = new BufferedReader(isr);
				final char endMarker = '!';

				// StringBuffer if you need to be threadsafe
				StringBuilder messageBuffer = new StringBuilder();
				int value;
				// reads to the end of the stream or till end of message
				while ((value = br.read()) != -1) {
					char c = (char) value;
					if (c == endMarker) {
						break;
					} else {
						messageBuffer.append(c);
					}
				}
				String message = messageBuffer.toString();

				if (message != null) {

					// Create a new thread for every message received to process
					// it after register is complete. Else, go to register.
					if (reg_complete) {
						System.out.println("Message from " + hostName + " is "
								+ message);
						final String msg = message;
						Thread yz = new Thread() {
							public void run() {
								try {
									String[] list = msg.split(" ");
									msg_type(list);
								} catch (NumberFormatException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
						};
						yz.start();
					}

					// Sending the process its id.
					else if (message.equalsIgnoreCase("register")) {
						System.out.println("Message from " + hostName + " is "
								+ message);
						String returnMessage;
						if (active_process < no_process) {

							// Assign a id to the process and send it the
							// information.
							// Also store its hostname.
							active_process++;
							String id = Integer.toString(active_process);
							returnMessage = "ACK " + id;

							// Store the id and the hostname in the map.
							id_hostnames.put(id, hostName);

							// Sending the response back to the process.
							OutputStream os = socket.getOutputStream();
							OutputStreamWriter osw = new OutputStreamWriter(os);
							BufferedWriter bw = new BufferedWriter(osw);
							System.out.println("Message sent is : "
									+ returnMessage);
							bw.write(returnMessage + "!");
							bw.flush();
						}
					}
				}
				if (active_process == no_process && !reg_complete) {
					reg_complete = true;
					Thread one = new Thread() {
						public void run() {
							coord_q();
						}
					};
					one.start();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				// socket.close();
			} catch (Exception e) {
			}
		}
	}

	// Perform some action depending on the type of the message received.
	public static void msg_type(String[] list) {

		// Receive the first commit message.
		if (list[0].equalsIgnoreCase("Request")) {
			t_id = Integer.valueOf(list[1]);
			value = Integer.valueOf(list[2]);
			try {
				send_msg(coordinator, 25555, "agreed");
				try {
					BufferedWriter WriteFile = new BufferedWriter(
							new FileWriter(stateFile));
					WriteFile.write("W");
					WriteFile.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				Thread.sleep(5000);
				if (!prepare) {
					cohort_abort("Prepare not received");
				}
				prepare = false;
			} catch (Exception e) {
				cohort_abort("Prepare not received");
			}
		}

		// On receiving agreed message.
		else if (list[0].equalsIgnoreCase("agreed")) {
			agreed_count++;
			if (agreed_count == no_process) {
				agreed = true;
				coord_w();
				agreed_count = 0;
			}
		}

		// On receiving abort message.
		else if (list[0].equalsIgnoreCase("abort")) {
			if (is_coord) {
				coord_abort();
			} else {
				cohort_abort("Abort recived");
			}
		}

		// On receiving abort message.
		else if (list[0].equalsIgnoreCase("prepare")) {
			prepare = true;
			try {
				send_msg(coordinator, 25555, "ack");
				try {
					BufferedWriter WriteFile = new BufferedWriter(
							new FileWriter(stateFile));
					WriteFile.write("P");
					WriteFile.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				Thread.sleep(5000);
				if (!commit) {
					cohort_abort("Commit not received");
				}
				commit = false;
			} catch (Exception e) {
				cohort_abort("Cordinator down");
			}
		}

		// On receiving abort message.
		else if (list[0].equalsIgnoreCase("ack")) {
			ack_count++;
			if (ack_count == no_process) {
				ack = true;
				coord_p();
				ack_count = 0;
			}
		}

		// On receiving abort message.
		else if (list[0].equalsIgnoreCase("commit")) {
			commit = true;
			cohort_commit();
		}

		// exit
		else if (list[0].equalsIgnoreCase("exit")) {
			System.exit(0);
		}
	}

	// The following functions simulate the computing process of coordinator.
	@SuppressWarnings("resource")
	public static void coord_q() {

		try {
			Thread.sleep(2000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		// Read the input value to be committed.
		Scanner reader = new Scanner(System.in);
		System.out.print("Enter value to be committed : ");
		value = reader.nextInt();

		String msg;
		if (value == 0) {
			msg = "exit";
		} else {
			msg = "Request " + t_id + " " + value;
		}

		// Send a commit message to all cohorts.
		Iterator it = id_hostnames.entrySet().iterator();

		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			String id_proc = (String) pair.getKey();
			String host = (String) pair.getValue();
			try {
				System.out.println("SENT " + msg + " to " + host);
				send_msg(host, 25555, msg);			
			} catch (Exception e) {
				System.out.println("One of the cohorts is down! Aborting.");
				coord_abort();
			}
		}

		if (value == 0) {
			System.exit(0);
		}
		
		try {
			BufferedWriter WriteFile = new BufferedWriter(new FileWriter(
					stateFile));
			WriteFile.write("W");
			WriteFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try{
			Thread.sleep(5000);
			if(!agreed){
				coord_abort();
			}
			agreed = false;
		}catch(Exception e){
			//To Do
		}
	}

	public static void coord_w() {

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		// Send prepare to all the cohorts.
		Iterator it = id_hostnames.entrySet().iterator();
		String msg = "Prepare";
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			String id_proc = (String) pair.getKey();
			String host = (String) pair.getValue();
			try {
				System.out.println("SENT " + msg + " to " + host);
				send_msg(host, 25555, msg);
			} catch (Exception e) {
				System.out.print("One of the cohorts is down! Aborting.");
				coord_abort();
			}
		}
		try {
			BufferedWriter WriteFile = new BufferedWriter(new FileWriter(
					stateFile));
			WriteFile.write("P");
			WriteFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		try{
			Thread.sleep(5000);
			if(!ack){
				coord_abort();
			}
			ack = false;
		}catch(Exception e){
			//To Do
		}
	}

	public static void coord_p() {

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		// Send commit to all the cohorts.
		Iterator it = id_hostnames.entrySet().iterator();
		String msg = "Commit";
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			String id_proc = (String) pair.getKey();
			String host = (String) pair.getValue();
			try {
				System.out.println("SENT " + msg + " to " + host);
				send_msg(host, 25555, msg);
			} catch (Exception e) {
				System.out.print("One of the cohorts is down! Committing.");
			}
		}
		coord_commit();
	}

	public static void coord_commit() {
		System.out.println("!!!!!! Transaction Committed !!!!!!!");
		try {
			BufferedWriter WriteFile = new BufferedWriter(new FileWriter(
					"commit_" + id + ".txt", true));
			WriteFile.write("Commit " + t_id + " " + value);
			WriteFile.write("\n");
			WriteFile.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
		t_id++;
		coord_q();
	}

	public static void coord_abort() {

		aborting = true;
		System.out.println("Aborting !");
		// Send a abort message to all cohorts.
		Iterator it = id_hostnames.entrySet().iterator();
		String msg = "abort";
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			String id_proc = (String) pair.getKey();
			String host = (String) pair.getValue();
			try {
				System.out.println("SENT " + msg + " to " + host);
				send_msg(host, 25555, msg);
			} catch (Exception e) {
				if (!aborting) {
					System.out.print("One of the cohorts is down! Aborting.");
					coord_abort();
				}
			}
		}

	}

	// The following functions simulate the computing process of cohort.
	public static void cohort_commit() {
		System.out.println("!!!!!! Transaction Committed !!!!!!!");
		try {
			BufferedWriter WriteFile = new BufferedWriter(new FileWriter(
					"commit_" + id + ".txt", true));
			WriteFile.write("Commit " + t_id + " " + value);
			WriteFile.write("\n");
			WriteFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void cohort_abort(String emsg) {
		System.out.println("Aborted ! " + emsg);
	}

}
