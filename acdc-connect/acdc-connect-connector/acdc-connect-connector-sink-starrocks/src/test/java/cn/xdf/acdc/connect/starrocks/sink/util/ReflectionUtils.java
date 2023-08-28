package cn.xdf.acdc.connect.starrocks.sink.util;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ReflectionUtils {
    
    /**
     * Get limited access field.
     *
     * @param object object
     * @param fieldName field name
     * @return field value
     * @throws NoSuchFieldException no such field exception
     * @throws IllegalAccessException illegal access exception
     */
    public static Object getLimitedAccessField(final Object object, final String fieldName) throws NoSuchFieldException, IllegalAccessException {
        Class objectClass = object.getClass();
        Field objectField = objectClass.getDeclaredField(fieldName);
        objectField.setAccessible(true);
        return objectField.get(object);
    }
    
    /**
     * Get limited access method execute result without args.
     *
     * @param object object
     * @param methodName method name
     * @return method result
     * @throws InvocationTargetException invocation target exception
     * @throws IllegalAccessException illegal access exception
     * @throws NoSuchMethodException no such method exception
     */
    public static Object getLimitedAccessMethodExecuteResultWithoutArgs(final Object object, final String methodName) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        Class objectClass = object.getClass();
        Method declaredMethod = objectClass.getDeclaredMethod(methodName);
        declaredMethod.setAccessible(true);
        return declaredMethod.invoke(object);
    }
    
}
