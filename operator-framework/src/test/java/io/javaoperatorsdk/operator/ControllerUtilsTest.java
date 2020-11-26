package io.javaoperatorsdk.operator;

import io.fabric8.kubernetes.client.CustomResourceDoneable;
import io.javaoperatorsdk.operator.api.Context;
import io.javaoperatorsdk.operator.api.Controller;
import io.javaoperatorsdk.operator.api.ResourceController;
import io.javaoperatorsdk.operator.api.UpdateControl;
import io.javaoperatorsdk.operator.sample.TestCustomResource;
import io.javaoperatorsdk.operator.sample.TestCustomResourceController;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControllerUtilsTest {

    public static final String CUSTOM_FINALIZER_NAME = "a.custom/finalizer";

    @Test
    public void returnsValuesFromControllerAnnotationFinalizer() {
        final var controller = new TestCustomResourceController(null);
        final var configuration = controller.getConfiguration();
        assertEquals(TestCustomResourceController.CRD_NAME, configuration.getCRDName());
        assertEquals(ControllerUtils.getDefaultFinalizerName(configuration.getCRDName()), configuration.getFinalizer());
        assertEquals(TestCustomResource.class, configuration.getCustomResourceClass());
        assertFalse(configuration.isGenerationAware());
        assertTrue(CustomResourceDoneable.class.isAssignableFrom(ControllerUtils.getCustomResourceDoneableClass(controller)));
    }

    @Controller(crdName = "test.crd", finalizerName = CUSTOM_FINALIZER_NAME)
    static class TestCustomFinalizerController implements ResourceController<TestCustomResource> {

        @Override
        public boolean deleteResource(TestCustomResource resource, Context<TestCustomResource> context) {
            return false;
        }

        @Override
        public UpdateControl<TestCustomResource> createOrUpdateResource(TestCustomResource resource, Context<TestCustomResource> context) {
            return null;
        }
    }

    @Test
    public void returnCustomerFinalizerNameIfSet() {
        assertEquals(CUSTOM_FINALIZER_NAME, new TestCustomFinalizerController().getConfiguration().getFinalizer());
    }
}
