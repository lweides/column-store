package column.store.api;

import java.util.*;

public class Utils {

    private Utils() {
        // hidden utils constructor
    }

    public static <T> List<Class<? extends T>> subclasses(Class<T> clazz) {
        var allClasses = new ArrayList<Class<? extends T>>();
        var stack = new ArrayDeque<Class<?>>();
        stack.push(clazz);
        while (!stack.isEmpty()) {
            var current = stack.pop();
            allClasses.add((Class<? extends T>) current);
            var subClasses = current.getPermittedSubclasses();
            if (subClasses != null) {
                Collections.addAll(stack, subClasses);
            }
        }
        return allClasses;
    }
}
