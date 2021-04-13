package com.yuepong.jdev.pulsar.transfer;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.MessageIdImpl;
import com.yuepong.jdev.pulsar.common.Config;
import com.yuepong.jdev.pulsar.kit.ArgsResolver;


/**
 * @ClassName: PulsarConsumers
 * @Description:
 * @Author: apr
 * @Date: 2021/04/09 08:44:54
 **/
public class PulsarConsumers {

	private static PulsarClient client;

	public static void main(String args[]) throws Exception {

		String serviceUrl = ArgsResolver.getParam(args, "url", Config.SERVICE_URL);
		String from = ArgsResolver.getParam(args, "from", "");
		String to = ArgsResolver.getParam(args, "to", "");
		long ledgerId = Long.parseLong( ArgsResolver.getParam(args, "lid", Config.LEDGER_ID) );
		long entryId = Long.parseLong( ArgsResolver.getParam(args, "eid", Config.ENTRY_ID) );
		int partitionIndex = Integer.parseInt( ArgsResolver.getParam(args, "pid", Config.PARTITION_INDEX) );

		System.out.printf("\r\n");
		System.out.printf("**************************************************\r\n");
		System.out.printf("Param\turl\tSERVICE_URL\t\t%s", serviceUrl + "\r\n");
		System.out.printf("Param\tlid\tLEDGER_ID\t\t%s", ledgerId + "\r\n");
		System.out.printf("Param\teid\tENTRY_ID\t\t%s", entryId + "\r\n");
		System.out.printf("Param\tpid\tPARTITION_INDEX\t\t%s", partitionIndex + "\r\n");
		System.out.printf("Param\tfrom\tCONSUME\t\t%s", from + "\r\n");
		System.out.printf("Param\tto\tPRODUCE\t\t%s", to + "\r\n");
		System.out.printf("**************************************************\r\n");

		client = PulsarClient.builder().serviceUrl(serviceUrl).build();
		MessageId id = new MessageIdImpl(ledgerId, entryId, partitionIndex);
		Reader<byte[]> reader = client.newReader().topic(from)
				.startMessageId(id)
				.create();

		long count=1;

		while (true) {
			Message msg = reader.readNext();

			Producer<byte[]> producer = client.newProducer().topic(to).create();
			producer.send(msg.getData());

			System.out.printf("Transfering: %s\tCount: %s\tmessage:\r\n", msg.getMessageId(), count++);
			System.out.printf(new String(msg.getData(),"UTF-8") + "\r\n");
		}
	}
}
