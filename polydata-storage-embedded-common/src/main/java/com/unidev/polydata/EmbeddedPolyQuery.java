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


import com.unidev.polydata.domain.PolyQuery;
import lombok.*;

/**
 * Object for holding poly query object
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class EmbeddedPolyQuery implements PolyQuery {

    public static final Long DEFAULT_ITEM_PER_PAGE = 30L;

    @Getter
    @Setter
    private String partition;

    @Getter
    @Setter
    private String tag;

    @Getter
    @Setter
    @Builder.Default
    private Long page = 0L;

    @Getter
    @Setter
    @Builder.Default
    private Long itemPerPage = DEFAULT_ITEM_PER_PAGE;

    @Getter
    @Setter
    private Boolean randomOrder;


}
