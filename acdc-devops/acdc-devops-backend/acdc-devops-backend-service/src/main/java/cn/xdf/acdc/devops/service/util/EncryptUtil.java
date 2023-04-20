package cn.xdf.acdc.devops.service.util;

import cn.xdf.acdc.devops.service.error.SystemBizException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.codec.binary.Base64;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

/**
 * A CDC devops encrypt util.
 */
public final class EncryptUtil {

    // 秘钥盐 key
    private static final String SALT_KEY = "MHbSHtzcVBRS7yJA";

    // 初始化向量key
    private static final String VECTOR_KEY = "fUvNFah4qHvgwhxl";

    // 加密算法.
    private static final String ALGORITHM = "AES";

    // Cipher实例初始化参数.
    private static final String INSTANCE = "AES/CBC/PKCS5Padding";

    private EncryptUtil() {

    }

    /**
     * 加密.
     *
     * @param content 需要加密的内容
     * @return 加密的后的 base64 字符串
     */
    public static String encrypt(final String content) {

        return encrypt(content, SALT_KEY, VECTOR_KEY);
    }

    /**
     * 加密.
     *
     * @param content 加密内容
     * @param saltKey 加密时使用的盐，16位字符串
     * @param vectorKey 加密时使用的向量，16位字符串
     * @return java.lang.String
     *
     * @date 2020-05-15 11:07
     */
    public static String encrypt(final String content, final String saltKey, final String vectorKey) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(content), "Content must not be null.");
        try {
            byte[] encrypted;
            Cipher cipher = Cipher.getInstance(INSTANCE);
            SecretKey secretKey = new SecretKeySpec(saltKey.getBytes(), ALGORITHM);
            IvParameterSpec iv = new IvParameterSpec(vectorKey.getBytes());
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, iv);
            encrypted = cipher.doFinal(content.getBytes());
            return Base64.encodeBase64String(encrypted);
        } catch (NoSuchAlgorithmException
            | NoSuchPaddingException
            | InvalidKeyException
            | InvalidAlgorithmParameterException
            | IllegalBlockSizeException
            | BadPaddingException e) {
            throw new SystemBizException(e);
        }
    }

    /**
     * 解密.
     *
     * @param base64Content 已加密的 base64 字符串
     * @return 解密后的内容
     */
    public static String decrypt(final String base64Content) {
        return decrypt(base64Content, SALT_KEY, VECTOR_KEY);
    }


    /**
     * 解密.
     *
     * @param base64Content 解密内容(base64编码格式)
     * @param saltKey 加密时使用的盐，16位字符串
     * @param vectorKey 加密时使用的向量，16位字符串
     * @return java.lang.String
     *
     * @date 2020-05-15 11:07
     */
    public static String decrypt(final String base64Content, final String saltKey, final String vectorKey) {

        Preconditions.checkArgument(!Strings.isNullOrEmpty(base64Content), "Base64 content must not be null.");

        try {
            byte[] encrypted;
            Cipher cipher = Cipher.getInstance(INSTANCE);
            SecretKey secretKey = new SecretKeySpec(saltKey.getBytes(), ALGORITHM);
            IvParameterSpec iv = new IvParameterSpec(vectorKey.getBytes());
            cipher.init(Cipher.DECRYPT_MODE, secretKey, iv);
            byte[] content = Base64.decodeBase64(base64Content);
            encrypted = cipher.doFinal(content);
            return new String(encrypted);
        } catch (NoSuchAlgorithmException
            | NoSuchPaddingException
            | InvalidKeyException
            | InvalidAlgorithmParameterException
            | IllegalBlockSizeException
            | BadPaddingException e) {
            throw new SystemBizException(e);
        }
    }
}
