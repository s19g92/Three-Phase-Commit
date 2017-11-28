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
class raymond_tree {

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
	static Map<String, String> parent = new HashMap<String, String>();
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
				} else {
					parent.put(list[0], list[1]);
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
			String[] list = message.split(" ");
			int q = Integer.valueOf(list[1]);
			
			boolean in = false;
			for(String x : queue) {
				if(x.equalsIgnoreCase(list[1])){
					in = true;
				}
			}
			if(!in)
				queue.add(list[1]);
			
			if (has_token && !in_cs) {
				// if (queue.get(0).equalsIgnoreCase(list[1])) {
				Timestamp time = new Timestamp(System.currentTimeMillis());
				String msg = "Token " + sdf.format(time);
				has_token = false;
				holder = queue.get(0);
				queue.remove(0);
				System.out.println("SENT : " + msg + " to " + holder);
				send_msg(my_nebr_hostnames.get(holder), 25555, msg);

				// }
			} else if (!has_token && !req_sent) {
				String msg = "";
				request_time = new Timestamp(System.currentTimeMillis());
				msg = "Request " + id;
				msg_count++;
				req_sent = true;
				send_msg(my_nebr_hostnames.get(holder), 25555, msg);
			}
			System.out.print("QUEUE : " + queue);
			System.out.println("");

		}

		// On receiving token
		else if (message.split(" ")[0].equalsIgnoreCase("token")) {

			String[] list = message.split(" ");

			if (!queue.isEmpty()) {
				if (queue.get(0).equalsIgnoreCase(id)) {
					queue.remove(0);
					SimpleDateFormat format = new SimpleDateFormat("HH.mm.ss");
					Date date1 = (Date) format.parse(""
							+ sdf.format(request_time));
					Date date2 = (Date) format.parse(list[1]);
					Date date3 = (Date) format.parse(""
							+ sdf.format(new Timestamp(System
									.currentTimeMillis())));

					float difference = date2.getTime() - date1.getTime();
					wait_time = date3.getTime() - date1.getTime();
					delay = date3.getTime() - date2.getTime();
					has_token = true;
					req_sent = false;
					start_cs();
				} else {

					holder = queue.get(0);
					queue.remove(0);
					req_sent = false;
					has_token = false;
					send_msg(my_nebr_hostnames.get(holder), 25555, message);
					if (!queue.isEmpty()) {
						String msg = "";
						request_time = new Timestamp(System.currentTimeMillis());
						msg = "Request " + id;
						msg_count++;
						req_sent = true;
						send_msg(my_nebr_hostnames.get(holder), 25555, msg);

					}
				}
			}
			System.out.print("QUEUE : " + queue);
			System.out.println("");

		}

		// On receiving data from other process.
		else if (message.split(" ")[0].equalsIgnoreCase("data")) {
			String[] list = message.split(" ");

			avg_msg_count = (avg_msg_count * cs_counter + Math.abs(Float
					.parseFloat(list[1]))) / (cs_counter + 1);
			avg_delay = (avg_delay * cs_counter + Math.abs(Float
					.parseFloat(list[2]))) / (cs_counter + 1);
			avg_wait_time = (avg_wait_time * cs_counter + Math.abs(Float
					.parseFloat(list[2]))) / (cs_counter + 1);
			cs_counter++;
		}

		// On receiving completed from process.
		else if (message.equalsIgnoreCase("completed")) {
			complete++;
			if (complete == no_process) {
				Iterator it = id_hostnames.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry pair = (Map.Entry) it.next();
					String id_proc = (String) pair.getKey();
					String host = (String) pair.getValue();
					String msg = "TERMINATE";
					if (!id_proc.equalsIgnoreCase("0")) {
						System.out.println("SENT " + msg + " to " + host);
						send_msg(host, 25555, msg);
					}

				}
				System.out.println("AVG MSG COUNT : " + avg_msg_count);
				System.out.println("AVG DELAY : " + avg_delay);
				System.out.println("AVG WAIT TIME : " + avg_wait_time);
				System.out.println("Token : " + has_token);
				System.exit(0);
			}

		}

		// On Receiving compute message.
		else if (message.equalsIgnoreCase("compute")) {
			compute();
		}

		// On receiving terminate.
		else if (message.equalsIgnoreCase("terminate")) {
			System.out.println("Token : " + has_token);
			System.exit(0);
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
							returnMessage = "ACK " + id + " " + parent.get(id);

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
		try {
			// Randomly sleep the thread for upto 5ms.
			int randomNum = ThreadLocalRandom.current().nextInt(t1, t2 + 1);
			Thread.sleep(randomNum);
			queue.add(id);

			// After waking up request critical section if it doesnt have token
			if (!has_token) {
				if (!req_sent) {
					req_sent = true;
					String msg = "";
					request_time = new Timestamp(System.currentTimeMillis());
					msg = "Request " + id;
					msg_count++;				
					send_msg(my_nebr_hostnames.get(holder), 25555, msg);
					System.out.print("SENT : " + msg + " to " + holder);

				}
			}

			else if (has_token) {

				if (queue.get(0).equalsIgnoreCase(id)) {
					queue.remove(0);
					start_cs();
				} else {
					holder = queue.get(0);
					queue.remove(0);
					has_token = false;
					Timestamp time = new Timestamp(System.currentTimeMillis());
					String msg = "Token " + sdf.format(time);
					System.out.print("SENT : " + msg);
					send_msg(my_nebr_hostnames.get(holder), 25555, msg);
					System.out.println("");
					if (!queue.isEmpty()) {
						req_sent = true;
						String re_msg = "";
						request_time = new Timestamp(System.currentTimeMillis());
						re_msg = "Request " + id;
						msg_count++;				
						System.out
								.println("SENT : " + re_msg + " to " + holder);
						send_msg(my_nebr_hostnames.get(holder), 25555, re_msg);

					}
				}
			}

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void start_cs() throws InterruptedException {

		in_cs = true;

		System.out.println("ENTERING CS !!");
		Thread.sleep(t3);
		System.out.println("EXITING CS !!");

		if (!is_coord) {
			String data_msg = "DATA " + msg_count + " " + delay + " "
					+ wait_time;
			System.out.println("SENT : " + data_msg + " to coordinator");
			send_msg(my_nebr_hostnames.get("0"), 25555, data_msg);

		} else {
			String data_msg = "DATA " + msg_count + " " + delay + " "
					+ wait_time;
			System.out.println("ADDED : " + data_msg + " to average");
			avg_msg_count = (avg_msg_count * cs_counter + msg_count)
					/ (cs_counter + 1);
			avg_delay = (avg_delay * cs_counter + delay) / (cs_counter + 1);
			avg_wait_time = (avg_wait_time * cs_counter + wait_time)
					/ (cs_counter + 1);
			cs_counter++;
		}

		// Reset data
		msg_count = 0;
		delay = 0;
		wait_time = 0;

		// If queue is not empty. Send the token.
		if (!queue.isEmpty()) {
			// Change the parent node.
			holder = queue.get(0);
			queue.remove(0);
			Timestamp time = new Timestamp(System.currentTimeMillis());
			has_token = false;
			String msg = "Token " + sdf.format(time);
			System.out.println("SENT : " + msg + " to " + holder);
			send_msg(my_nebr_hostnames.get(holder), 25555, msg);
			System.out.println("");

			if (!queue.isEmpty()) {
				String re_msg = "";
				request_time = new Timestamp(System.currentTimeMillis());
				re_msg = "Request " + id;
				msg_count++;
				req_sent = true;
				System.out.println("SENT : " + re_msg + " to " + holder);
				send_msg(my_nebr_hostnames.get(holder), 25555, re_msg);
			}
		}
		System.out.print("QUEUE : " + queue);
		System.out.println("");

		in_cs = false;

		run_count--;
		System.out.println("RUN COUNT LEFT : " + run_count);
		if (run_count > 0) {
			compute();
		} else {
			if (!is_coord)
				send_msg(my_nebr_hostnames.get("0"), 25555, "completed");
			else {
				complete++;
				if (complete == no_process) {
					Iterator it = id_hostnames.entrySet().iterator();
					while (it.hasNext()) {
						Map.Entry pair = (Map.Entry) it.next();
						String id_proc = (String) pair.getKey();
						String host = (String) pair.getValue();
						String msg = "TERMINATE";
						if (!id_proc.equalsIgnoreCase("0")) {

							System.out.print("SENT " + msg + " to " + host);
							System.out.println("");
							send_msg(host, 25555, msg);
						}
					}
					System.out.println("AVG MSG COUNT" + avg_msg_count);
					System.out.println("AVG DELAY" + avg_delay);
					System.out.println("AVG WAIT TIME" + avg_wait_time);
					System.out.println("Token : " + has_token);
					System.exit(0);
				}
			}
		}
	}
}