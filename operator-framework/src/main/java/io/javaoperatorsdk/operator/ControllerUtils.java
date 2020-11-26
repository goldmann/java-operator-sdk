package io.javaoperatorsdk.operator;

import java.util.Map;

import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceDoneable;
import io.javaoperatorsdk.operator.api.ResourceController;


public class ControllerUtils {
    
    private static final String FINALIZER_NAME_SUFFIX = "/finalizer";
    
    public static final String CONTROLLERS_RESOURCE_PATH = "javaoperatorsdk/controllers";
    private static Map<Class<? extends ResourceController>, Class<? extends CustomResource>> controllerToCustomResourceMappings;
    
    static {
        controllerToCustomResourceMappings = ControllerToCustomResourceMappingsProvider.provide(CONTROLLERS_RESOURCE_PATH);
    }
    
    public static String getDefaultFinalizerName(String crdName) {
        return crdName + FINALIZER_NAME_SUFFIX;
    }
    
    public static <R extends CustomResource> Class<R> getCustomResourceClass(ResourceController<R> controller) {
        final Class<? extends CustomResource> customResourceClass = controllerToCustomResourceMappings.get(controller.getClass());
        if (customResourceClass == null) {
            throw new IllegalArgumentException(
                String.format(
                    "No custom resource has been found for controller %s",
                    controller.getClass().getCanonicalName()
                )
            );
        }
        return (Class<R>) customResourceClass;
    }
    
    public static <T extends CustomResource> Class<? extends CustomResourceDoneable<T>>
    getCustomResourceDoneableClass(ResourceController<T> controller) {
        try {
            final Class<T> customResourceClass = getCustomResourceClass(controller);
            return (Class<? extends CustomResourceDoneable<T>>) Class.forName(customResourceClass.getCanonicalName() + "Doneable");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }
    
    public static boolean hasGivenFinalizer(CustomResource resource, String finalizer) {
        return resource.getMetadata().getFinalizers() != null && resource.getMetadata().getFinalizers().contains(finalizer);
    }
}
