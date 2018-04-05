package ru.csc.bdse.util;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 * @author semkagtn
 */
public final class Encoding {

    private Encoding() {

    }

    public static String encodeUrl(String url) {
        try {
            return URLEncoder.encode(url, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
