package cn.itcast.hotel.controller;

import cn.itcast.hotel.pojo.RequestParam;
import cn.itcast.hotel.pojo.ResponseHotel;
import cn.itcast.hotel.service.IHotelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/hotel")
public class HotelController {

    @Autowired
    private IHotelService hotelService;

    @RequestMapping("/list")
    public ResponseHotel list(@RequestBody RequestParam requestParam){
        return hotelService.search(requestParam);
    }

    @RequestMapping("/filters")
    public Map<String, List<String>> filters(@RequestBody RequestParam requestParam){
        return hotelService.filters(requestParam);
    }


    @RequestMapping("/suggestion")
    public List<String> suggestion(@org.springframework.web.bind.annotation.RequestParam("key") String key){
        return hotelService.suggestion(key);
    }
}
