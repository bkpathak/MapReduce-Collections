package org.bkpathak.mapreduce.iplookup;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The mapper class uses the maxmind geoip2 API for the geoLookup of the ip address.
 * The map method will parse the log file and geoLocation method will resolve the ip
 * address and stored it in the Hash Map which the map can then use to emit the records.
 */
public class GeoLocationLookupMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  Logger logger = LoggerFactory.getLogger(GeoLocationLookupMapper.class);

  // A File object pointing to your GeoIP2 or GeoLite2 database
  private File database;

  // This creates the DatabaseReader object, which will be reused across lookups.
  private DatabaseReader reader;

  private static int FIELDS = 1;
  private InetAddress ipAddress;
  private Map<String, String> ipLocation;

  //For getting response from the City database.
  CityResponse response;
  Country country;
  Subdivision subdivision;
  City city;
  Postal postal;
  Location location;

  private static final IntWritable one = new IntWritable(1);
  private Text locationEmit = new Text();

  /**
   * Compile the given regular expression into a pattern
   */
  Pattern pattern = Pattern.compile("((?:\\d{1,3}\\.){3}\\d{1,3})");
  Matcher matcher;

  public GeoLocationLookupMapper() {
    this.ipAddress = null;
    this.ipLocation = new HashMap<>();
    this.response = null;
    this.subdivision = null;
    this.city = null;
    this.postal = null;
    this.location = null;


  }

  @Override
  protected void setup(Context context) throws IOException {
    logger.info("Mapper's Setup method");
    Configuration conf = context.getConfiguration();
    String geoipFileName = conf.get("geoip.filename");
    File file = new File(geoipFileName);
    reader = new DatabaseReader.Builder(file).build();

  }

  public void geoLocationLookup() throws GeoIp2Exception, IOException {

    response = reader.city(ipAddress);

    // Put the country name and iso name in ipLocation
    country = response.getCountry();
    ipLocation.put("country", country.getName());
    //ipLocation.put("country_iso_code", country.getIsoCode());

    // put the subdivision and iso in ipLocation
    //subdivision = response.getMostSpecificSubdivision();
    //ipLocation.put("subdivision", subdivision.getName());
    //ipLocation.put("subdivision_iso_code", subdivision.getIsoCode());

    // put the city name in ipLocation
    city = response.getCity();
    ipLocation.put("city", city.getName());

    //put the postal code in ipLocation
    //postal = response.getPostal();
    //ipLocation.put("postal", postal.getCode());

    // put latitude and longitude in ipLocation
    //location = response.getLocation();
    //ipLocation.put("latitude", location.getLatitude().toString());
    //ipLocation.put("longitude", location.getLongitude().toString());
  }

  @Override
  public void map(LongWritable key, Text value, Context context)
          throws IOException, InterruptedException {

    String line = value.toString();
    matcher = pattern.matcher(line);
    if (!matcher.matches() || FIELDS != matcher.groupCount()) {
      logger.warn("Unable to parse");
    } else {
      ipAddress = InetAddress.getByName(matcher.group());
      try {
        geoLocationLookup();
      } catch (GeoIp2Exception e) {
        e.printStackTrace();
      }
    }
    for (Map.Entry<String, String> entry : ipLocation.entrySet()) {
      locationEmit.set(entry.getValue());
      context.write(locationEmit, one);
    }
  }

}

