package com.dch.tutorial.beam.util;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to log each element of {@link PCollection} with additional prefix.
 *
 * @author david.christianto
 */
public class LogUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogUtil.class);

    private LogUtil() {
    }

    @SuppressWarnings("unchecked")
    public static <T> PTransform<PCollection<T>, PCollection<T>> ofElements() {
        return new LogUtil.LoggingTransform();
    }

    @SuppressWarnings("unchecked")
    public static <T> PTransform<PCollection<T>, PCollection<T>> ofElements(String prefix) {
        return new LogUtil.LoggingTransform(prefix);
    }

    /**
     * Class to log message to {@link Logger}.
     *
     * @param <T> Data type.
     */
    private static class LoggingTransform<T> extends PTransform<PCollection<T>, PCollection<T>> {

        private String prefix;

        private LoggingTransform() {
            this.prefix = "";
        }

        private LoggingTransform(String prefix) {
            this.prefix = prefix;
        }

        public PCollection<T> expand(PCollection<T> input) {
            return input.apply(ParDo.of(new DoFn<T, T>() {
                @ProcessElement
                public void processElement(@Element T element, OutputReceiver<T> out, BoundedWindow window) {
                    String message = LoggingTransform.this.prefix + element.toString();
                    if (!(window instanceof GlobalWindow)) {
                        message = message + "  Window:" + window.toString();
                    }

                    LOGGER.info(message);
                    out.output(element);
                }
            }));
        }
    }
}
