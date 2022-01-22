package cn.itcast.hotel.service;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.RequestParam;
import cn.itcast.hotel.pojo.ResponseHotel;
import com.baomidou.mybatisplus.extension.service.IService;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.util.List;

public interface IHotelService extends IService<Hotel> {
    ResponseHotel search(RequestParam requestParam);
}
