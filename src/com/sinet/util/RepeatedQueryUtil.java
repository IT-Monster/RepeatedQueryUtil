package com.sinet.util;

import org.codehaus.jackson.map.ObjectMapper;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;
import redis.clients.jedis.JedisPool;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Clob;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RepeatedQueryUtil {

    private static double similarityvalue = 0.85;
    /**
     * 文本长度高于textLength使用智能分词,低于textLength使用最细粒度分词
     */
    private static int textLength = 100;



    /**
     * 分词并保存到文件
     * @param outputStream
     * @param list
     * @throws Exception
     */
    public static void saveToFileIK(FileOutputStream outputStream , List<Map<String,Object>> list) throws Exception {
        BufferedWriter writer;
        writer = new BufferedWriter(new OutputStreamWriter(outputStream,"UTF-8"));
        String content = null;
        for (Map<String, Object> map : list) {
            try {
                // 默认KEY , BIEC_CLOBCOTENT
                content = getObjToStr(map.get("BIEC_CLOBCOTENT"));
                Map<String, Integer> participles = delHTMLandParticipleIK(content);
                map.put("BIEC_CLOBCOTENT",participles);
                String json = Obj2Json(map);
                writer.write(json + "\r\n");
            } catch (Exception e) {
                System.out.println("ID : " + map.get("ID")+" : "+e.getMessage());
            }
        }
        writer.flush();
        writer.close();
    }


    /**
     * 加载文件中的数据
     * @param fileInputStream
     * @return
     * @throws Exception
     */
    public static List<Map<String,Object>> LoadedIntoTheCache(FileInputStream fileInputStream) throws Exception {
        BufferedReader reader;
        List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
        try {
            reader = new BufferedReader(new InputStreamReader(fileInputStream, "UTF-8"));
            String str;
            while ((str = reader.readLine()) != null) {
                Map<String, Object> map = json2Map(str);
                list.add(map);
            }
            return list;
        }catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }



    /**
     * 将Map中的BIEC_CLOBCOTENT文本分词
     * @param list
     * @return
     */
    public static List<Map<String,Object>> MapTextToParticiples(List<Map<String,Object>> list){

        if (list == null || list.size() == 0)  {
            return null;
        }
        String str = "";
        for (Map<String, Object> map : list) {
            try {
                str = getObjToStr(map.get("BIEC_CLOBCOTENT"));
                map.put("BIEC_CLOBCOTENT",delHTMLandParticipleIK(str));
            } catch (Exception e) {
                System.out.println(map.get("ID"));
            }
        }
        return list;
    }

    /**
     * 返回词频高于xxx的词
     * @param words
     * @return
     */
    public static Map<String, Integer> getHighWordFrequency(Map<String,Integer> words){
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (Map.Entry<String, Integer> entry : words.entrySet()) {
            Integer value = entry.getValue();
            if (value > 10) {
                map.put(entry.getKey(),value);
            }
        }
        return map;
    }


    /**
     * 去除字符串HTML标签, IK分词 文本长度大于textLength使用智能分词,小于则使用最细粒度分词
     * @param text
     * @return
     * @throws Exception
     */
    public static Map<String, Integer> delHTMLandParticipleIK(String text) throws Exception{
        try {
            Map<String, Integer> profile ;
            if (text.length() < textLength) {
                profile  = participleik(delHTMLTag(text));
            }else {
                profile  = participleikSmart(delHTMLTag(text));
            }
            return profile;
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
    }


    //******************************Ik分词.计算词频*************************************

    /**
     * IK分词_智能分词
     * @param text
     * @return
     * @throws Exception
     */
    public static Map<String, Integer> participleikSmart(String text) throws Exception {
        if ( text == null || "".equals(text) || text.length() <= 3 ){
            throw new Exception("文本过短");
        }
        Map<String, Integer> wordsFren=new HashMap<String, Integer>();
        IKSegmenter ikSegmenter = new IKSegmenter(new StringReader(text), true);
        Lexeme lexeme;
        while ((lexeme = ikSegmenter.next()) != null) {
//            if(lexeme.getLexemeText().length()>1){
            if(wordsFren.containsKey(lexeme.getLexemeText())){
                wordsFren.put(lexeme.getLexemeText(),wordsFren.get(lexeme.getLexemeText())+1);
            }else {
                wordsFren.put(lexeme.getLexemeText(),1);
            }
        }
//        }
        return wordsFren;
    }

    /**
     * IK分词_最细粒度分词
     * @param text
     * @return
     * @throws Exception
     */
    public static Map<String, Integer> participleik(String text) throws Exception {
        if ( text == null || "".equals(text) || text.length() <= 3){
            throw new Exception("文本过短");
        }
        Map<String, Integer> wordsFren=new HashMap<String, Integer>();
        IKSegmenter ikSegmenter = new IKSegmenter(new StringReader(text), false);
        Lexeme lexeme;
        while ((lexeme = ikSegmenter.next()) != null) {
            if(wordsFren.containsKey(lexeme.getLexemeText())){
                wordsFren.put(lexeme.getLexemeText(),wordsFren.get(lexeme.getLexemeText())+1);
            }else {
                wordsFren.put(lexeme.getLexemeText(),1);
            }
        }
        return wordsFren;
    }



    //********************************计算相似度*****************************************

    public static double similarity(Map<String, Integer> profile1, Map<String, Integer> profile2) {
        return dotProduct(profile1, profile2)  / (norm(profile1) * norm(profile2));
    }

    private static double dotProduct( final Map<String, Integer> profile1, final Map<String, Integer> profile2) {

        Map<String, Integer> small_profile = profile2;
        Map<String, Integer> large_profile = profile1;
        if (profile1.size() < profile2.size()) {
            small_profile = profile1;
            large_profile = profile2;
        }

        double agg = 0;
        for (Map.Entry<String, Integer> entry : small_profile.entrySet()) {
            Integer i = large_profile.get(entry.getKey());
            if (i == null ) {
                continue;
            }
            agg += 1.0 * entry.getValue() * i;
        }
        return agg;
    }

    private static double norm(final Map<String, Integer> profile) {
        double agg = 0;

        for (Map.Entry<String, Integer> entry : profile.entrySet()) {
            agg += 1.0 * entry.getValue() * entry.getValue();
        }
        return Math.sqrt(agg);
    }



    //*********************************默认分词方案***************************************

    //分词长度
    private static int k = 3;

    private static final Pattern SPACE_REG = Pattern.compile("\\s+");

    /**
     * 默认分词 + 计算词频
     * @param string
     * @return
     * @throws Exception
     */
    @Deprecated
    public static Map<String, Integer> getProfile(final String string) throws Exception{

        if ( string == null || "".equals(string) || string.length() <= k ){
            throw new Exception("文本过短");
        }
        HashMap<String, Integer> shingles = new HashMap<String, Integer>();

        String string_no_space = SPACE_REG.matcher(string).replaceAll("");
        for (int i = 0; i < (string_no_space.length() - k + 1); i++) {
            String shingle = string_no_space.substring(i, i + k);
            Integer old = shingles.get(shingle);
            if (old != null) {
                shingles.put(shingle, old + 1);
            } else {
                shingles.put(shingle, 1);
            }
        }

        return Collections.unmodifiableMap(shingles);
    }

    /**
     * 去除字符串HTML标签,并分词  默认分词
     * @param text
     * @return
     */
    @Deprecated
    public static Map<String, Integer> delHTMLandParticiple(String text) throws Exception{
        try {
            Map<String, Integer> profile = getProfile(delHTMLTag(text));
            return profile;
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
    }

    /**
     * 保存到文件_默认分词方案
     * @param outputStream
     * @param list
     * @throws IOException
     */
    @Deprecated
    public static void saveToFile(FileOutputStream outputStream , List<Map<String,Object>> list) throws Exception {
        BufferedWriter writer;
        writer = new BufferedWriter(new OutputStreamWriter(outputStream,"UTF-8"));
        String content = null;
        for (Map<String, Object> map : list) {
            try {
                content = getObjToStr(map.get("BIEC_CLOBCOTENT"));
                Map<String, Integer> participles = delHTMLandParticiple(content);
                map.put("BIEC_CLOBCOTENT",participles);
                String json = Obj2Json(map);
                writer.write(json + "\r\n");
            } catch (Exception e) {
                System.out.println("ID : " + map.get("ID")+" : "+e.getMessage());
            }
        }
        writer.flush();
        writer.close();
    }

    /**
     * 将Map中的BIEC_CLOBCOTENT文本分词默认分词
     * @param list
     * @return
     */
    @Deprecated
    public static List<Map<String,Object>> MapTextToParticiplesDef(List<Map<String,Object>> list){

        if (list == null || list.size() == 0)  {
            return null;
        }
        String str = "";
        for (Map<String, Object> map : list) {
            try {
                str = getObjToStr(map.get("BIEC_CLOBCOTENT"));
                map.put("BIEC_CLOBCOTENT",delHTMLandParticiple(str));
            } catch (Exception e) {
                System.out.println(map.get("ID"));
            }
        }
        return list;
    }

    /**
     * 查重
     * @param list
     * @param text
     * @return
     */
    public static  List<Map<String, String>> repeatedQuery(List<Map<String,Object>> list,Map<String, Integer> participle){
        List<Map<String, String>> res = new ArrayList<>();
        try {
            for (Map<String, Object> map : list) {
                double similarity = similarity(participle, (Map<String, Integer>) map.get("BIEC_CLOBCOTENT"));
                if (similarity > similarityvalue) {
                    Map<String, String> hashMap = new HashMap<String, String>();
                    hashMap.put("ID",map.get("ID").toString());
                    hashMap.put("BIB_ID",map.get("BIB_ID").toString());
                    hashMap.put("SIMILARITY",similarity+"");
                    res.add(hashMap);
                }
            }
        }catch (Exception e) {
        }
        return res;
    }

    //***************************去富文本HTML标签***************************************

    // 定义script的正则表达式
    private static final String regEx_script = "<script[^>]*?>[\\s\\S]*?<\\/script>";
    // 定义style的正则表达式
    private static final String regEx_style = "<style[^>]*?>[\\s\\S]*?<\\/style>";
    // 定义HTML标签的正则表达式
    private static final String regEx_html = "<[^>]+>";
    //定义空格回车换行符
    private static final String regEx_space = "\\s*|\t|\r|\n";
    //全角空格
    private static final char regEx_12288 = 12288;

    /**
     * 去除文本的HTML标签
     * @param htmlStr
     * @return
     */
    public static String delHTMLTag(String htmlStr){

        Pattern p_script = Pattern.compile(regEx_script, Pattern.CASE_INSENSITIVE);
        Matcher m_script = p_script.matcher(htmlStr);
        htmlStr = m_script.replaceAll(""); // 过滤script标签

        Pattern p_style = Pattern.compile(regEx_style, Pattern.CASE_INSENSITIVE);
        Matcher m_style = p_style.matcher(htmlStr);
        htmlStr = m_style.replaceAll(""); // 过滤style标签

        Pattern p_html = Pattern.compile(regEx_html, Pattern.CASE_INSENSITIVE);
        Matcher m_html = p_html.matcher(htmlStr);
        htmlStr = m_html.replaceAll(""); // 过滤html标签

        Pattern p_space = Pattern.compile(regEx_space, Pattern.CASE_INSENSITIVE);
        Matcher m_space = p_space.matcher(htmlStr);
        htmlStr = m_space.replaceAll(""); // 过滤空格回车标签

        htmlStr = htmlStr.replaceAll("&nbsp;", "");
        htmlStr = htmlStr.replaceAll(String.valueOf(regEx_12288), "");
        return htmlStr.trim(); // 返回文本字符串
    }


    //*******************************字符串相关方法****************************************

    private static ObjectMapper objectMapper = new ObjectMapper();

    private static String Obj2Json(Object obj) throws Exception {
        try {
            StringWriter stringWriter = new StringWriter();
            objectMapper.writeValue(stringWriter,obj);
            return stringWriter.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static String getObjToStr(Object obj) {
        if (obj == null) {
            return null;
        }
        String result = "";
        if (obj instanceof Integer) {
            result = String.valueOf(obj);
        } else if (obj instanceof BigDecimal) {
            result = ((BigDecimal) obj).toString();
        } else if (obj instanceof BigInteger) {
            result = String.valueOf(obj);
        } else if (obj instanceof Long) {
            result = String.valueOf(obj);
        } else if (obj instanceof Boolean) {
            result = String.valueOf(obj);
        } else if (obj instanceof Short) {
            result = String.valueOf(obj);
        } else if (obj instanceof Float) {
            result = String.valueOf(obj);
        } else if (obj instanceof Double) {
            result = String.valueOf(obj);
        } else if (obj instanceof String) {
            result = String.valueOf(obj);
        } else if (obj instanceof String[]) {
            String[] str = (String[]) obj;
            if (str.length > 0) {
                result = str[0];
            }
        } else if (obj instanceof Clob) {
            try {
                result = getClobToString((Clob) obj);
            } catch (Exception e) {
            }
        } else {
            try {
                result = String.valueOf(obj);
            } catch (Exception e) {
            }
        }
        return result;
    }

    private static String getClobToString(Clob clob) {
        String reString = "";
        Reader is = null;
        BufferedReader br = null;
        try {
            is = clob.getCharacterStream();
            br = new BufferedReader(is);
            String s = br.readLine();
            StringBuffer sb = new StringBuffer();
            int i = 0;
            while (s != null) {
                if (i == 0) {
                    sb.append(s);
                } else {
                    sb.append("\n").append(s);
                }
                i++;
                s = br.readLine();
            }
            reString = sb.toString();
        } catch (Exception e) {
        } finally {
            if (is != null) {
                try {
                    br.close();
                    is.close();
                } catch (Exception e) {
                }
            }
        }
        return reString;
    }

    private static Map<String,Object> json2Map(String json) {
        try {
            Object o = objectMapper.readValue(json, Map.class);
            return (Map<String,Object>)o;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static Object json2Obj(String json,Class clazz){
        try {
            Object o = objectMapper.readValue(json, clazz);
            return o;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void setSimilarityValue(double similarityvalue) {
        RepeatedQueryUtil.similarityvalue = similarityvalue;
    }

    public static void setTextLength(int textLength) {
        RepeatedQueryUtil.textLength = textLength;
    }
}
