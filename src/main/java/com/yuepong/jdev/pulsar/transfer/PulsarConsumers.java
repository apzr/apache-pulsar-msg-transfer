package com.yuepong.jdev.pulsar.transfer;

import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class PulsarConsumers {

	private static PulsarClient client;

	public static void main(String args[]) throws Exception {

		String serviceUrl = ArgsResolver.getParam(args, "url", Config.SERVICE_URL);
		String from = ArgsResolver.getParam(args, "from", "");
		String to = ArgsResolver.getParam(args, "to", "");
		long ledgerId = Long.parseLong( ArgsResolver.getParam(args, "lid", Config.LEDGER_ID) );
		long entryId = Long.parseLong( ArgsResolver.getParam(args, "eid", Config.ENTRY_ID) );
		int partitionIndex = Integer.parseInt( ArgsResolver.getParam(args, "pid", Config.PARTITION_INDEX) );

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

			log.info("Transferring: {}\tCount: {}", msg.getMessageId(), count++);
			log.debug("Message:\t{}", new String(msg.getData(),"UTF-8"));
		}
	}
}
