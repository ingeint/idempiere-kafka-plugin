package com.ingeint.kafka.event;

import org.compiere.model.MBPartner;

import com.ingeint.kafka.base.CustomEvent;
import com.ingeint.kafka.presenter.Partner;
import com.ingeint.kafka.producer.PartnerKafkaProducer;

public class NewPartnerEvent extends CustomEvent {

	private final PartnerKafkaProducer producer = new PartnerKafkaProducer();

	@Override
	protected void doHandleEvent() {
		MBPartner mbPartner = (MBPartner) getPO();
		producer.send(createPartnerPresenter(mbPartner));
	}

	private Partner createPartnerPresenter(MBPartner mbPartner) {
		Partner partner = new Partner();
		partner.setUu(mbPartner.getC_BPartner_UU());
		partner.setName(mbPartner.getName());
		return partner;
	}

}
