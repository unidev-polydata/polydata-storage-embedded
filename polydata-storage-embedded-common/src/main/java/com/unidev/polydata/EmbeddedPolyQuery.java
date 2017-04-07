package com.unidev.polydata;


/**
 * Object for holding poly query object
 */
public class EmbeddedPolyQuery {

    public static final Long DEFAULT_ITEM_PER_PAGE = 30L;

    private String tag;
    private Long page = 0L;
    private Long itemPerPage = DEFAULT_ITEM_PER_PAGE;
    private Boolean randomOrder;

    public Boolean getRandomOrder() {
        return randomOrder;
    }

    public void setRandomOrder(Boolean randomOrder) {
        this.randomOrder = randomOrder;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public Long getPage() {
        return page;
    }

    public void setPage(Long page) {
        this.page = page;
    }

    public Long getItemPerPage() {
        return itemPerPage;
    }

    public void setItemPerPage(Long itemPerPage) {
        this.itemPerPage = itemPerPage;
    }
}
