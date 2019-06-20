package com.dch.tutorial.beam.transform;

import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.util.Random;

/**
 * Class that responsible to generate unbounded records.
 *
 * @author david.christianto
 */
public class GenerateFruit extends PTransform<PBegin, PCollection<String>> {

    private GenerateFruit() {
    }

    public static GenerateFruit randomly() {
        return new GenerateFruit();
    }

    @Override
    public PCollection<String> expand(PBegin input) {
        return input
                .apply(GenerateSequence.from(0).withRate(1, Duration.standardSeconds(1)))
                .apply("Generate random string",
                        ParDo.of(new DoFn<Long, String>() {
                            @ProcessElement
                            public void processElement(@Element Long number, OutputReceiver<String> out) {
                                int rand = new Random().nextInt(2);
                                String result = rand == 1 ? "apple" : "orange";
                                out.output(result);
                            }
                        }));
    }
}
