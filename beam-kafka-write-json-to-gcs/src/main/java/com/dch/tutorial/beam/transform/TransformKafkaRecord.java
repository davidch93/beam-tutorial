package com.dch.tutorial.beam.transform;

import org.apache.beam.sdk.transforms.DoFn;

/**
 * Transform the kafka record to specific record. The record used format from Debezium.
 *
 * @author david.christianto
 */
public class TransformKafkaRecord extends DoFn<String, String> {

}
