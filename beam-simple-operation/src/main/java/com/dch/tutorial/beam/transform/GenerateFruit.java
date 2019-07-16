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
 * @see org.apache.beam.sdk.transforms.PTransform
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
                                int rand = new Random().nextInt(5);
                                switch (rand) {
                                    case 0:
                                        out.output("apple");
                                        break;
                                    case 1:
                                        out.output("banana");
                                        break;
                                    case 2:
                                        out.output("grape");
                                        break;
                                    case 3:
                                        out.output("watermelon");
                                        break;
                                    case 4:
                                        out.output("melon");
                                        break;
                                    default:
                                        out.output("orange");
                                        break;
                                }
                            }
                        }));
    }
}
