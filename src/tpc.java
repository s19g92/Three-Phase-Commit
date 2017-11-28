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
	static int t1;
	static int t2;
	static int t3;
	static int no_process;
	static boolean is_coord = false;
	static final SimpleDateFormat sdf = new SimpleDateFormat("HH.mm.ss");

	// Coordinator only data.
	static int active_process = 0;
	static int ready_process = 0;
	static float avg_msg_count = 0;
	static float avg_wait_time = 0;
	static float avg_delay = 0;
	static float cs_counter = 0;
	static int complete;
	static Map<String, String> id_hostnames = new HashMap<String, String>();

	// Individual Process Variables.
	static String id = "";
	static int run_count = 0;
	static boolean has_token = false;
	static boolean in_cs = false;
	static boolean reg_complete = false;
	static boolean req_sent = false;
	static Socket socket;
	static String holder = "";
	static List<String> queue = new ArrayList<String>();
	static Map<String, String> my_nebr_hostnames = new HashMap<String, String>();

	// Data Variables.
	static int msg_count = 0;
	static long wait_time;
	static long delay;
	static Timestamp request_time;

	/****************************** MAIN FUNCTION *********************************************/

	public static void main(String[] args) throws Exception {

		if (args.length != 0) {
			if (args[0].equalsIgnoreCase("-c")) {
				is_coord = true;
				id = "0";
				active_process++;
			}
		}

		Random r = new Random();
		int Low = 5;
		int High = 15;
		run_count = r.nextInt(High - Low) + Low;

		// Read the dsConfig File and assign the data to proper variables.
		Scanner sc2 = null;
		String[] list = null;
		try {
			sc2 = new Scanner(new File("RdsConfig"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		while (sc2.hasNextLine()) {
			String next = sc2.nextLine();
			if (!next.isEmpty()) {
				list = next.split("\\s");
				if (list[0].equalsIgnoreCase("coordinator")) {
					coordinator = list[1];
					if (is_coord) {
						id_hostnames.put("0", coordinator + ".utdallas.edu");
						has_token = true;
					}
				} else if (list[0].equalsIgnoreCase("number")) {
					no_process = Integer.valueOf(list[3]);
				} else if (list[0].equalsIgnoreCase("t1")) {
					t1 = Integer.valueOf(list[1]);
				} else if (list[0].equalsIgnoreCase("t2")) {
					t2 = Integer.valueOf(list[1]);
				} else if (list[0].equalsIgnoreCase("t3")) {
					t3 = Integer.valueOf(list[1]);
				} 
			}
		}
		System.out.println("Coordinator : " + coordinator);
		System.out.println("Is coordinator : " + is_coord);
		System.out.println("No. of process :" + no_process);
		System.out.println("Token:" + has_token);
		System.out.println("HOLDER:" + holder);
		System.out.println("RUN TIMES : " + run_count);
		System.out.println("");

		// If the node is the coordinator then start listener.
		Thread start = new Thread() {
			public void run() {
				try {
					if (is_coord) {
						coord_register_new();
						recv_msg();
						// If the node is not the coordinator send register.
					} else {
						process_register();
						recv_msg();
					}
					System.out.println("Program thread exited");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
		start.start();
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
	public static void recv_msg() throws IOException {

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
								msg_type(msg);
							} catch (IOException e) {
								e.printStackTrace();
							} catch (InterruptedException e) {
								e.printStackTrace();
							} catch (NumberFormatException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (ParseException e) {
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

	// Perform some action depending on the type of the message received.
	public static void msg_type(String message) throws IOException,
			InterruptedException, NumberFormatException, ParseException {
		// On Receiving a hostnames from store in map.
		if (message.split(" ")[0].equalsIgnoreCase("hostnames")) {
			String[] list = message.split(" ");
			for (int i = 1; i < list.length - 1; i++) {
				String id = list[i];
				i++;
				if (!list[i].isEmpty()) {
					my_nebr_hostnames.put(id, list[i]);
				}
			}
			reg_complete = true;
			System.out.print(my_nebr_hostnames);
			System.out.println("");
		}

		// On receiving request message.
		else if (message.split(" ")[0].equalsIgnoreCase("request")) {
		

		}

		// On receiving token
		else if (message.split(" ")[0].equalsIgnoreCase("token")) {

		

		}

		// On receiving data from other process.
		else if (message.split(" ")[0].equalsIgnoreCase("data")) {
		
		}

		// On receiving completed from process.
		else if (message.equalsIgnoreCase("completed")) {
			
		}

		// On Receiving compute message.
		else if (message.equalsIgnoreCase("compute")) {
			
		}

		// On receiving terminate.
		else if (message.equalsIgnoreCase("terminate")) {
			
		}
	}

	// Send the inital register message to the coordinator
	// if the process itself is not the coordinator.
	public static void process_register() throws IOException {

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
						holder = msg_list[2];
						System.out.println("Id received : " + id);
						System.out.println("Parent received : " + holder);
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
	public static void coord_register_new() throws IOException {

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
									msg_type(msg);
								} catch (IOException e) {
									e.printStackTrace();
								} catch (InterruptedException e) {
									e.printStackTrace();
								} catch (NumberFormatException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								} catch (ParseException e) {
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
							String id = Integer.toString(active_process - 1);
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
					System.out.println("All process registered !");
					send_hostnames();
					reg_complete = true;
					start_compute();
					Thread one = new Thread() {
						public void run() {
							compute();
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

	// Send the hostnames of the neighbours to registered processes
	// once all processes have registered and their hostnames are stored.
	public static void send_hostnames() throws IOException {

		// If all processes sent the register message and coordinator
		// knows everyone's hostname.
		if (active_process == no_process) {

			// Coordinator registers its own neighbours
			if (is_coord) {
				my_nebr_hostnames = id_hostnames;
			}

			// Send hostnames of neightbours to processes.
			for (int i = 1; i <= no_process - 1; i++) {
				String message = "Hostnames";

				Iterator it = id_hostnames.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry pair = (Map.Entry) it.next();
					String id = (String) pair.getKey();
					String host = (String) pair.getValue();
					message = message + " " + id + " " + host;

				}
				send_msg(id_hostnames.get("" + i), 25555, message);
			}
		}
	}

	// Send the compute message after receiving all ready messages.
	public static void start_compute() {

		Iterator it = id_hostnames.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			String id_proc = (String) pair.getKey();
			String host = (String) pair.getValue();
			String msg = "COMPUTE";
			if (!id_proc.equalsIgnoreCase("0")) {
				System.out.println("SENT " + msg + " to " + host);
				send_msg(host, 25555, msg);
			}
		}

	}

	// Simulates the computing process of a node on receiving a message.
	public static void compute() {
	}
	
	}