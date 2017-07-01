/**
 * Copyright (c) 2017 Denis O <denis.o@linux.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.unidev.polydata;

import static com.unidev.polydata.EmbeddedPolyConstants.POLY_OBJECT_MAPPER;

import com.unidev.polydata.domain.BasicPoly;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.util.Collection;
import java.util.Optional;
import java.util.Random;

public class SQLiteStorageFlatFile extends SQLiteStorage {

  public static final String FILES_FOLDER = "json";

  public SQLiteStorageFlatFile(String dbFile) {
    super(dbFile);
  }

  static final String DICTIONARY = "abcdefghiklmnopqrstxyz0123456789";
  static final int PATH_LENGTH = 3;

  String randomDictionaryChar(Random random, String seed) {
    int charId = random.nextInt(DICTIONARY.length());
    return DICTIONARY.charAt(charId) + "";
  }

  String fetchPath(String id) {
    Random random = new Random(id.hashCode());
    String path = "";
    for (int length = 1; length <= PATH_LENGTH; length++) {
      String part = "";
      for(int i = 0;i<length;i++) {
        part += randomDictionaryChar(random, id);
      }
      path += part + File.separator;
    }
    return path;
  }

  @Override
  public Optional<BasicPoly> fetchPoly(Connection connection, String id) {
    File fileFolder = fetchFlatFilePath(id);
    File file = new File(fileFolder, id);
    if (!file.exists()) {
      return Optional.empty();
    }
    try {
      return Optional.of(POLY_OBJECT_MAPPER.readValue(file, BasicPoly.class));
    } catch (IOException e) {
      throw new EmbeddedStorageException(e);
    }
  }

  @Override
  public BasicPoly persistPoly(Connection connection, BasicPoly poly) {
    String id = poly._id();
    File fileFolder = fetchFlatFilePath(id);
    fileFolder.mkdirs();

    File jsonFile = new File(fileFolder, id);

    try {
      POLY_OBJECT_MAPPER.writeValue(jsonFile, poly);
    } catch (IOException e) {
      throw new EmbeddedStorageException(e);
    }

    BasicPoly persistedPoly = BasicPoly.newPoly(poly._id());
    Collection tags = poly.fetch(EmbeddedPolyConstants.TAGS_KEY);
    persistedPoly.put(EmbeddedPolyConstants.TAGS_KEY, tags);

    super.persistPoly(connection, persistedPoly);

    return poly;
  }

  @Override
  public boolean removePoly(Connection connection, String polyId) {
    File fileFolder = fetchFlatFilePath(polyId);
    fileFolder.mkdirs();
    File jsonFile = new File(fileFolder, polyId);
    jsonFile.delete();
    return removeRawPoly(connection, EmbeddedPolyConstants.DATA_POLY, polyId);
  }


  File fetchFlatFilePath(String id) {
    String filePath = fetchPath(id);
    File parent = new File(dbFile).getParentFile();
    return new File(parent, FILES_FOLDER + File.separator + filePath);
  }

}
